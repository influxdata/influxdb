package organizations

import (
	"context"
	"fmt"
)

type contextKey string

// ContextKey is the key used to specify the
// organization via context
const ContextKey = contextKey("organization")

func validOrganization(ctx context.Context) error {
	// prevents panic in case of nil context
	if ctx == nil {
		return fmt.Errorf("expect non nil context")
	}
	orgID, ok := ctx.Value(ContextKey).(string)
	// should never happen
	if !ok {
		return fmt.Errorf("expected organization key to be a string")
	}
	if orgID == "" {
		return fmt.Errorf("expected organization key to be set")
	}
	return nil
}
