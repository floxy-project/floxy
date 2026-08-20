package floxy

import (
	"errors"
)

var (
	ErrEntityNotFound    = errors.New("entity not found")
	ErrQueueItemLockLost = errors.New("queue item lock lost")
)
