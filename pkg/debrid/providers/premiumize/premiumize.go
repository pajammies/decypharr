package premiumize

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	json "github.com/bytedance/sonic"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

const (
	apiBase              = "https://www.premiumize.me/api"
	directDLCacheTTL     = 720 * time.Hour // 30 days - links are persistent in Premiumize
	transferListCacheTTL = 5 * time.Second
)

type Premiumize struct {
	config               config.Debrid
	apiKey               string
	client               *request.Client
	logger               zerolog.Logger
	accountsManager      *account.Manager
	profile              *types.Profile
	profileLastFetched   time.Time
	profileCacheDuration time.Duration

	// Cache for directdl results keyed by transfer source
	directDLCache       *xsync.Map[string, *CachedLink]
	transferListCache   *xsync.Map[string, time.Time]
	transferListCacheMu sync.RWMutex
	cachedTransfers     []*types.Torrent
	cachedTransfersTime time.Time
}

// New creates a new Premiumize client
func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Premiumize, error) {
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	}

	_log := logger.New(dc.Name)

	cfg := config.Get()
	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithMaxRetries(cfg.Retries),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithRetryableStatus(http.StatusTooManyRequests),
		request.WithProxy(dc.Proxy),
	}

	p := &Premiumize{
		config:               dc,
		apiKey:               dc.APIKey,
		client:               request.New(opts...),
		logger:               _log,
		accountsManager:      account.NewManager(dc, ratelimits["download"], _log),
		profileCacheDuration: 1 * time.Hour,
		directDLCache:        xsync.NewMap[string, *CachedLink](),
		transferListCache:    xsync.NewMap[string, time.Time](),
	}

	// Fetch profile in background
	go func() {
		_, _ = p.GetProfile()
	}()

	return p, nil
}

// SubmitMagnet submits a magnet link to Premiumize
func (p *Premiumize) SubmitMagnet(tr *types.Torrent) (*types.Torrent, error) {
	if tr == nil || tr.Magnet.Link == "" {
		return nil, fmt.Errorf("invalid torrent or magnet link")
	}

	data := url.Values{}
	data.Set("src", tr.Magnet.Link)
	payload := bytes.NewBufferString(data.Encode())

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/transfer/create", apiBase), payload)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to submit magnet: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp apiError
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("premiumize API error: %s (%s)", errResp.Message, errResp.Code)
	}

	var createResp transferCreateResponse
	if err := json.Unmarshal(body, &createResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if createResp.Status != "success" {
		return nil, fmt.Errorf("transfer creation failed")
	}

	// Return transfer as torrent with initial status
	result := &types.Torrent{
		Id:       createResp.ID,
		Name:     createResp.Name,
		Debrid:   p.config.Name,
		Status:   types.TorrentStatusDownloading,
		InfoHash: tr.Magnet.InfoHash,
		Size:     tr.Magnet.Size,
		Progress: 0,
		Files:    make(map[string]types.File),
	}

	return result, nil
}

// CheckStatus checks the status of a transfer
func (p *Premiumize) CheckStatus(tr *types.Torrent) (*types.Torrent, error) {
	if tr == nil || tr.Id == "" {
		return nil, fmt.Errorf("invalid torrent or transfer ID")
	}

	transfers, err := p.GetTorrents()
	if err != nil {
		return nil, err
	}

	for _, t := range transfers {
		if t.Id == tr.Id {
			// Update download links if finished
			if t.Status == types.TorrentStatusDownloaded && len(t.Files) > 0 {
				for _, file := range t.Files {
					if file.Link == "" {
						// Try to fetch links
						_ = p.refreshTransferLinks(t)
						break
					}
				}
			}
			return t, nil
		}
	}

	return nil, fmt.Errorf("transfer not found: %s", tr.Id)
}

// GetDownloadLink gets a download link for a file using directdl
func (p *Premiumize) GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error) {
	if torrentID == "" || file == nil {
		return types.DownloadLink{}, fmt.Errorf("invalid torrent ID or file")
	}

	// Check cache first
	if cached, ok := p.directDLCache.Load(torrentID); ok {
		if time.Now().Before(cached.ExpiresAt) {
			// Find matching file in cached content
			for _, content := range cached.Content {
				if content.Path == file.Path {
					return types.DownloadLink{
						Link:      content.Link,
						Token:     content.Link,
						ExpiresAt: cached.ExpiresAt,
					}, nil
				}
			}
		} else {
			p.directDLCache.Delete(torrentID)
		}
	}

	// Get transfer info to find the source magnet/link
	transfers, err := p.GetTorrents()
	if err != nil {
		return types.DownloadLink{}, err
	}

	var transfer *types.Torrent
	for _, t := range transfers {
		if t.Id == torrentID {
			transfer = t
			break
		}
	}

	if transfer == nil {
		return types.DownloadLink{}, fmt.Errorf("transfer not found: %s", torrentID)
	}

	// If transfer is not finished, we can't get direct links
	if transfer.Status != types.TorrentStatusDownloaded {
		return types.DownloadLink{}, fmt.Errorf("transfer not yet downloaded: status=%s", transfer.Status)
	}

	// Use directdl with the magnet link
	data := url.Values{}
	var src string
	if transfer.Magnet != nil && transfer.Magnet.Link != "" {
		src = transfer.Magnet.Link
	} else if transfer.InfoHash != "" {
		src = utils.ConstructMagnet(transfer.InfoHash, transfer.Name).Link
	} else {
		return types.DownloadLink{}, fmt.Errorf("no magnet or infohash available for transfer %s", torrentID)
	}
	data.Set("src", src)
	payload := bytes.NewBufferString(data.Encode())

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/transfer/directdl", apiBase), payload)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("failed to get direct download links: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return types.DownloadLink{}, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp apiError
		_ = json.Unmarshal(body, &errResp)
		return types.DownloadLink{}, fmt.Errorf("premiumize API error: %s", errResp.Message)
	}

	var dlResp directDLResponse
	if err := json.Unmarshal(body, &dlResp); err != nil {
		return types.DownloadLink{}, fmt.Errorf("failed to parse response: %w", err)
	}

	if dlResp.Status != "success" {
		return types.DownloadLink{}, fmt.Errorf("directdl failed")
	}

	// Cache the result
	expiresAt := time.Now().Add(directDLCacheTTL)
	p.directDLCache.Store(torrentID, &CachedLink{
		TransferId: torrentID,
		Content:    dlResp.Content,
		ExpiresAt:  expiresAt,
	})

	// Find matching file
	for _, content := range dlResp.Content {
		if content.Path == file.Path {
			return types.DownloadLink{
				Link:      content.Link,
				Token:     content.Link,
				ExpiresAt: expiresAt,
			}, nil
		}
	}

	return types.DownloadLink{}, fmt.Errorf("file not found in transfer: %s", file.Path)
}

// DeleteTorrent deletes a transfer from Premiumize
func (p *Premiumize) DeleteTorrent(torrentId string) error {
	if torrentId == "" {
		return fmt.Errorf("invalid transfer ID")
	}

	data := url.Values{}
	data.Set("id", torrentId)
	payload := bytes.NewBufferString(data.Encode())

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/transfer/delete", apiBase), payload)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete transfer: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp apiError
		_ = json.Unmarshal(body, &errResp)
		return fmt.Errorf("premiumize API error: %s", errResp.Message)
	}

	var apiResp apiError
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if apiResp.Status != "success" {
		return fmt.Errorf("failed to delete transfer")
	}

	// Clear cache for this transfer
	p.directDLCache.Delete(torrentId)

	return nil
}

// IsAvailable checks if infohashes are available in Premiumize cache
func (p *Premiumize) IsAvailable(infohashes []string) map[string]bool {
	if len(infohashes) == 0 {
		return make(map[string]bool)
	}

	result := make(map[string]bool)
	const batchSize = 100

	for i := 0; i < len(infohashes); i += batchSize {
		end := i + batchSize
		if end > len(infohashes) {
			end = len(infohashes)
		}

		// Build form data with multiple items - convert hashes to magnet URIs
		data := url.Values{}
		hashByItem := make(map[string]string, end-i)
		for _, hash := range infohashes[i:end] {
			if hash == "" {
				continue
			}
			item := utils.ConstructMagnet(hash, "").Link
			data.Add("items[]", item)
			hashByItem[item] = hash
		}

		if len(data) == 0 {
			continue
		}

		payload := bytes.NewBufferString(data.Encode())
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/cache/check", apiBase), payload)
		if err != nil {
			p.logger.Warn().Err(err).Msg("Failed to create request for cache check")
			continue
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := p.client.Do(req)
		if err != nil {
			p.logger.Warn().Err(err).Msg("Failed to check cache availability")
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			p.logger.Warn().Err(err).Msg("Failed to read cache check response")
			continue
		}

		var cacheResp cacheCheckResponse
		if err := json.Unmarshal(body, &cacheResp); err != nil {
			p.logger.Warn().Err(err).Msg("Failed to parse cache check response")
			continue
		}

		// Map results back to infohashes
		items := data["items[]"]
		for idx, available := range cacheResp.Response {
			if idx < len(items) && available {
				result[hashByItem[items[idx]]] = true
			}
		}
	}

	return result
}

// UpdateTorrent updates torrent information
func (p *Premiumize) UpdateTorrent(torrent *types.Torrent) error {
	if torrent == nil || torrent.Id == "" {
		return fmt.Errorf("invalid torrent")
	}

	// Refresh from API
	updated, err := p.CheckStatus(torrent)
	if err != nil {
		return err
	}

	// Copy updated fields back
	*torrent = *updated
	return nil
}

// GetTorrent gets a single transfer by ID
func (p *Premiumize) GetTorrent(torrentId string) (*types.Torrent, error) {
	transfers, err := p.GetTorrents()
	if err != nil {
		return nil, err
	}

	for _, t := range transfers {
		if t.Id == torrentId {
			return t, nil
		}
	}

	return nil, fmt.Errorf("transfer not found: %s", torrentId)
}

// GetTorrents gets all transfers from Premiumize
func (p *Premiumize) GetTorrents() ([]*types.Torrent, error) {
	// Check cache
	p.transferListCacheMu.RLock()
	if time.Since(p.cachedTransfersTime) < transferListCacheTTL && len(p.cachedTransfers) > 0 {
		defer p.transferListCacheMu.RUnlock()
		return p.cachedTransfers, nil
	}
	p.transferListCacheMu.RUnlock()

	resp, err := p.client.Get(fmt.Sprintf("%s/transfer/list", apiBase))
	if err != nil {
		return nil, fmt.Errorf("failed to get transfers: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp apiError
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("premiumize API error: %s", errResp.Message)
	}

	var listResp transferListResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if listResp.Status != "success" {
		return nil, fmt.Errorf("failed to get transfers")
	}

	// Convert transfers to torrents
	torrents := make([]*types.Torrent, 0, len(listResp.Transfers))
	for _, transfer := range listResp.Transfers {
		// Reconstruct magnet link from Src field if available
		var magnetLink *utils.Magnet
		if transfer.Src != "" {
			magnetLink = &utils.Magnet{
				Link: transfer.Src,
			}
		}

		torrent := &types.Torrent{
			Id:       transfer.ID,
			Name:     transfer.Name,
			Debrid:   p.config.Name,
			Status:   p.mapTransferStatus(transfer.Status),
			Progress: transfer.Progress,
			Files:    make(map[string]types.File),
			Magnet:   magnetLink,
		}
		torrents = append(torrents, torrent)
	}

	// Cache results
	p.transferListCacheMu.Lock()
	p.cachedTransfers = torrents
	p.cachedTransfersTime = time.Now()
	p.transferListCacheMu.Unlock()

	return torrents, nil
}

// Config returns the provider config
func (p *Premiumize) Config() config.Debrid {
	return p.config
}

// Logger returns the provider logger
func (p *Premiumize) Logger() zerolog.Logger {
	return p.logger
}

// RefreshDownloadLinks refreshes cached download links
func (p *Premiumize) RefreshDownloadLinks() error {
	_, err := p.GetTorrents()
	return err
}

// CheckFile checks if a file is available
func (p *Premiumize) CheckFile(ctx context.Context, infohash, fileID string) error {
	if infohash == "" {
		return fmt.Errorf("invalid infohash")
	}

	result := p.IsAvailable([]string{infohash})
	if !result[infohash] {
		return fmt.Errorf("file not available in cache")
	}

	return nil
}

// AccountManager returns the account manager
func (p *Premiumize) AccountManager() *account.Manager {
	return p.accountsManager
}

// GetProfile gets account profile information
func (p *Premiumize) GetProfile() (*types.Profile, error) {
	// Check cache
	if p.profile != nil && time.Since(p.profileLastFetched) < p.profileCacheDuration {
		return p.profile, nil
	}

	resp, err := p.client.Get(fmt.Sprintf("%s/account/info", apiBase))
	if err != nil {
		return nil, fmt.Errorf("failed to get profile: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp apiError
		_ = json.Unmarshal(body, &errResp)
		return nil, fmt.Errorf("premiumize API error: %s", errResp.Message)
	}

	var accountInfo accountInfoResponse
	if err := json.Unmarshal(body, &accountInfo); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	profile := &types.Profile{
		Id:       1,
		Username: fmt.Sprintf("%d", accountInfo.customerIDInt64()),
		Email:    "", // Premiumize doesn't return email in /api/account/info
	}

	if accountInfo.PremiumUntil != nil {
		profile.Expiration = time.Unix(*accountInfo.PremiumUntil, 0)
	}

	p.profile = profile
	p.profileLastFetched = time.Now()

	return profile, nil
}

// GetAvailableSlots calculates available slots from account limit
func (p *Premiumize) GetAvailableSlots() (int, error) {
	resp, err := p.client.Get(fmt.Sprintf("%s/account/info", apiBase))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("failed to get account info")
	}

	var accountInfo accountInfoResponse
	if err := json.Unmarshal(body, &accountInfo); err != nil {
		return 0, err
	}

	// Calculate: (1.0 - limit_used) * 100 gives remaining capacity as 0-100
	slots := int((1.0 - accountInfo.LimitUsed) * 100)
	if slots < 0 {
		slots = 0
	}

	return slots, nil
}

// SyncAccounts syncs account details
func (p *Premiumize) SyncAccounts() {
	_, _ = p.GetProfile()
	_, _ = p.GetAvailableSlots()
}

// DeleteLink deletes a download link
func (p *Premiumize) DeleteLink(dl types.DownloadLink) error {
	if dl.Token == "" {
		return fmt.Errorf("invalid download link token")
	}

	// Extract file ID from the link if possible
	// For now, we'll skip deletion as Premiumize doesn't have a direct link deletion API
	// Links expire based on transfer lifecycle
	return nil
}

// SpeedTest performs a speed test
func (p *Premiumize) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider:  p.config.Name,
		TestedAt:  time.Now(),
		SpeedMBps: 0,
		LatencyMs: 0,
		BytesRead: 0,
	}

	// For now, perform a simple latency test
	start := time.Now()
	resp, err := p.client.Get(fmt.Sprintf("%s/account/info", apiBase))
	if err != nil {
		result.SpeedMBps = 0
		return result
	}
	defer resp.Body.Close()

	latency := time.Since(start)
	result.LatencyMs = latency.Milliseconds()

	return result
}

// SupportsCheck returns whether this provider supports file checking
func (p *Premiumize) SupportsCheck() bool {
	return true // Premiumize supports cache checking
}

// Helper functions

func (p *Premiumize) mapTransferStatus(status string) types.TorrentStatus {
	switch status {
	case "queued":
		return types.TorrentStatusDownloading
	case "running":
		return types.TorrentStatusDownloading
	case "seeding", "finished":
		return types.TorrentStatusDownloaded
	case "error":
		return types.TorrentStatusError
	default:
		return types.TorrentStatusDownloading
	}
}

func (p *Premiumize) refreshTransferLinks(torrent *types.Torrent) error {
	if torrent == nil || torrent.Id == "" {
		return fmt.Errorf("invalid torrent")
	}

	// Load from cache if available
	if cached, ok := p.directDLCache.Load(torrent.Id); ok {
		if time.Now().Before(cached.ExpiresAt) {
			// Update files with links from cache
			for _, content := range cached.Content {
				torrent.Files[content.Path] = types.File{
					Name:      content.Path,
					Path:      content.Path,
					Link:      content.Link,
					Size:      content.Size,
					TorrentId: torrent.Id,
				}
			}
			return nil
		}
		p.directDLCache.Delete(torrent.Id)
	}

	return nil
}
