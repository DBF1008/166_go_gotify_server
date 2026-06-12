package model

import "time"

// ElevateRequest parameters for client elevation.
//
// swagger:model ElevateRequest
type ElevateRequest struct {
	// How long the elevation should last, in seconds.
	//
	// required: true
	// example: 900
	DurationSeconds int `form:"durationSeconds" query:"durationSeconds" json:"durationSeconds" binding:"required"`
}

// DefaultElevationDuration is the initial elevation duration for new sessions.
// Referenced by session.BrowserSessionPolicy and session.NativeSessionPolicy.
var DefaultElevationDuration = time.Hour
