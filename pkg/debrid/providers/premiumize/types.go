package premiumize

import "time"

// Standard response envelope for all Premiumize API responses
type APIResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message,omitempty"`
	Code    string          `json:"code,omitempty"`
	Data    map[string]any  `json:"data,omitempty"` // For raw responses
}

// AccountInfo represents account information
type AccountInfo struct {
	Status       string `json:"status"`
	CustomerId   string `json:"customer_id"`
	PremiumUntil int64  `json:"premium_until"`
	LimitUsed    float64 `json:"limit_used"`
	BoosterPoints int64  `json:"booster_points"`
}

// Transfer represents a transfer (torrent/link download)
type Transfer struct {
	Id        string   `json:"id"`
	Name      string   `json:"name"`
	Status    string   `json:"status"` // queued, running, finished, seeding, error
	Progress  float64  `json:"progress"`
	Message   string   `json:"message"`
	FolderId  string   `json:"folder_id"`
	FileId    string   `json:"file_id"`
	CreatedAt int64    `json:"created_at,omitempty"`
	Size      int64    `json:"size,omitempty"`
}

// TransferListResponse represents response from /api/transfer/list
type TransferListResponse struct {
	Status    string      `json:"status"`
	Transfers []Transfer `json:"transfers"`
}

// TransferCreateResponse represents response from /api/transfer/create
type TransferCreateResponse struct {
	Status string `json:"status"`
	Id     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type,omitempty"` // "container" if it's a container file
	Content []any  `json:"content,omitempty"` // For container responses
}

// DirectDLContent represents a file in directdl response
type DirectDLContent struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	Link string `json:"link"`
}

// DirectDLResponse represents response from /api/transfer/directdl
type DirectDLResponse struct {
	Status  string            `json:"status"`
	Content []DirectDLContent `json:"content"`
}

// CacheCheckResponse represents response from /api/cache/check
type CacheCheckResponse struct {
	Status    string   `json:"status"`
	Response  []bool   `json:"response"`
	Filename  []string `json:"filename"`
	Filesize  []string `json:"filesize"`
}

// ItemDetails represents a file item
type ItemDetails struct {
	Status    string `json:"status"`
	Id        string `json:"id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	CreatedAt int64  `json:"created_at"`
	FolderId  string `json:"folder_id"`
	MimeType  string `json:"mime_type"`
	Link      string `json:"link"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// CachedLink represents a cached download link
type CachedLink struct {
	TransferId string
	Content    []DirectDLContent
	ExpiresAt  time.Time
}
