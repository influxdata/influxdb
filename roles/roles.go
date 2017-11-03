package roles

import (
	"context"
	"fmt"
)

type contextKey string

// ContextKey is the key used to specify the
// role via context
const ContextKey = contextKey("role")

func validRole(ctx context.Context) error {
	// prevents panic in case of nil context
	if ctx == nil {
		return fmt.Errorf("expect non nil context")
	}
	role, ok := ctx.Value(ContextKey).(string)
	// should never happen
	if !ok {
		return fmt.Errorf("expected role key to be a string")
	}
	switch role {
	// TODO(desa): make real roles
	case "member", "viewer", "editor", "admin":
		return nil
	default:
		return fmt.Errorf("expected role key to be set")
	}
}
