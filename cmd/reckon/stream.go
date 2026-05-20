package main

import (
	"context"
	"io"
	"time"
)

// pump drives a reckon-go server-stream (a value channel + a single-error
// channel) into the NDJSON frame protocol (DESIGN §5): a leading "ready"
// frame, one frame per value, then exactly one terminal "end" or "error".
//
// frame must return a map already tagged with its "type" (e.g. "event",
// "store_event"). A context cancellation (SIGINT/stdin EOF upstream) is a
// clean stop: it emits {"type":"end","reason":"client_eof"} and returns nil.
func pump[T any](ctx context.Context, w io.Writer, events <-chan T, errs <-chan error, frame func(T) map[string]any) error {
	line := func(m map[string]any) { _ = writeJSON(w, m, false) }

	line(map[string]any{"type": "ready", "at": time.Now().UTC().Format(time.RFC3339Nano)})

	for ev := range events {
		line(frame(ev))
	}

	err := <-errs
	switch {
	case err == nil:
		line(map[string]any{"type": "end", "reason": "eof"})
		return nil
	case isCanceled(err) || ctx.Err() != nil:
		line(map[string]any{"type": "end", "reason": "client_eof"})
		return nil
	default:
		ce := fromGRPC(err)
		line(map[string]any{"type": "error", "code": ce.Code, "message": ce.Msg})
		return ce
	}
}
