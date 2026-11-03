package domain

import "errors"

var (
	ErrNotFound              = errors.New("not found")
	ErrConflict              = errors.New("conflict")
	ErrValidation            = errors.New("validation failed")
	ErrRateLimited           = errors.New("rate limited")
	ErrTemporary             = errors.New("temporary failure")
	ErrInvalidOutput         = errors.New("invalid classifier output")
	ErrCapabilityUnsupported = errors.New("capability unsupported")
	ErrAutomationDisabled    = errors.New("automation disabled")
)
