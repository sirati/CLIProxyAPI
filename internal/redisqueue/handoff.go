package redisqueue

import (
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/handoff"
	sdkhandoff "github.com/router-for-me/CLIProxyAPI/v8/sdk/handoff"
)

// After a binary handoff the old process sends every usage payload of the
// requests it is still finishing to the new process, which owns the queue.

func init() {
	handoff.AddCapability(sdkhandoff.KindEventUsage)
	handoff.AddCapability(sdkhandoff.KindStateUsageQueue)
}

// HandoffUsageEvent is the payload of event.usage.v1.
type HandoffUsageEvent struct {
	Error   bool   `json:"error,omitempty"`
	Payload []byte `json:"payload"`
}

func forwardForHandoff(payload []byte, isError bool) bool {
	return handoff.Forward(sdkhandoff.KindEventUsage, HandoffUsageEvent{Error: isError, Payload: payload})
}

// ApplyHandoffEvent enqueues a payload the old process forwarded.
func ApplyHandoffEvent(event HandoffUsageEvent) {
	if event.Error {
		EnqueueError(event.Payload)
		return
	}
	Enqueue(event.Payload)
}

// HandoffQueueItem is one retained usage payload (state.usagequeue.v1).
type HandoffQueueItem struct {
	EnqueuedAt time.Time `json:"enqueued_at"`
	Payload    []byte    `json:"payload"`
}

// ExportHandoffQueue returns the usage payloads no subscriber has taken yet.
func ExportHandoffQueue() []HandoffQueueItem {
	global.mu.Lock()
	defer global.mu.Unlock()
	global.pruneLocked(time.Now())
	items := make([]HandoffQueueItem, 0, len(global.items)-global.head)
	for _, item := range global.items[global.head:] {
		items = append(items, HandoffQueueItem{EnqueuedAt: item.enqueuedAt, Payload: append([]byte(nil), item.payload...)})
	}
	return items
}

// ImportHandoffQueue puts payloads from the old process back in the queue,
// keeping their age so retention still applies.
func ImportHandoffQueue(items []HandoffQueueItem) {
	if !Enabled() {
		return
	}
	global.mu.Lock()
	defer global.mu.Unlock()
	for _, item := range items {
		global.items = append(global.items, queueItem{enqueuedAt: item.EnqueuedAt, payload: item.Payload})
	}
	global.pruneLocked(time.Now())
}
