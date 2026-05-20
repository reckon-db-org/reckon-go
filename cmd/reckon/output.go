package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cliError carries a stable lowercase code, a gRPC status (-1 when not
// RPC-derived), and the process exit code (DESIGN §6.1).
type cliError struct {
	Code string
	Msg  string
	GRPC int
	Exit int
}

func (e *cliError) Error() string { return e.Msg }

func usageErr(format string, a ...any) *cliError {
	return &cliError{Code: "usage", Msg: fmt.Sprintf(format, a...), GRPC: -1, Exit: 2}
}

// fromGRPC maps an RPC error to a cliError per the §6.1 table.
func fromGRPC(err error) *cliError {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &cliError{"internal", err.Error(), -1, 1}
	}
	c := st.Code()
	codeName, exit := "internal", 1
	switch c {
	case codes.NotFound:
		codeName, exit = "not_found", 3
	case codes.Unavailable:
		codeName, exit = "unavailable", 4
	case codes.DeadlineExceeded:
		codeName, exit = "timeout", 5
	case codes.InvalidArgument:
		codeName, exit = "invalid_argument", 7
	case codes.FailedPrecondition:
		codeName, exit = "precondition", 8
	case codes.AlreadyExists:
		codeName, exit = "already_exists", 9
	}
	return &cliError{codeName, st.Message(), int(c), exit}
}

// isCanceled reports whether err is a context cancellation (clean stream stop).
func isCanceled(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Canceled
}

// writeJSON emits a single value followed by a newline; HTML escaping off so
// payload strings stay readable.
func writeJSON(w io.Writer, v any, pretty bool) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if pretty {
		enc.SetIndent("", "  ")
	}
	return enc.Encode(v)
}

// emitErr writes the §6 error object to stderr and returns the exit code.
func emitErr(stderr io.Writer, err error) int {
	ce, ok := err.(*cliError)
	if !ok {
		ce = &cliError{"internal", err.Error(), -1, 1}
	}
	_ = writeJSON(stderr, map[string]any{
		"error": map[string]any{
			"code":        ce.Code,
			"message":     ce.Msg,
			"grpc_status": ce.GRPC,
			"details":     map[string]any{},
		},
	}, false)
	return ce.Exit
}
