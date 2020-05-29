# HTTP Handler Style Guide

### HTTP Handler
* Each handler should implement `http.Handler`
  - This can be done by embedding a [`httprouter.Router`](https://github.com/julienschmidt/httprouter)
  (a light weight HTTP router that supports variables in the routing pattern and matches against the request method)
* Required services should be exported on the struct

```go
// ThingHandler represents an HTTP API handler for things.
type ThingHandler struct {
	// embedded httprouter.Router as a lazy way to implement http.Handler
	*httprouter.Router

	ThingService         platform.ThingService
	AuthorizationService platform.AuthorizationService

	Logger               *zap.Logger
}
```

### HTTP Handler Constructor

* Routes should be declared in the constructor

```go
// NewThingHandler returns a new instance of ThingHandler.
func NewThingHandler() *ThingHandler {
	h := &ThingHandler{
		Router: httprouter.New(),
		Logger: zap.Nop(),
	}

	h.HandlerFunc("POST", "/api/v2/things", h.handlePostThing)
	h.HandlerFunc("GET", "/api/v2/things", h.handleGetThings)

	return h
}
```

### Route handlers (`http.HandlerFunc`s)

* Each route handler should have an associated request struct and decode function
* The decode function should take a `context.Context` and an `*http.Request` and return the associated route request struct

```go
type postThingRequest struct {
	Thing *platform.Thing
}

func decodePostThingRequest(ctx context.Context, r *http.Request) (*postThingRequest, error) {
	t := &platform.Thing{}
	if err := json.NewDecoder(r.Body).Decode(t); err != nil {
		return nil, err
	}

	return &postThingRequest{
		Thing: t,
	}, nil
}
```

* Route `http.HandlerFuncs` should separate the decoding and encoding of HTTP requests/response from actual handler logic

```go
// handlePostThing is the HTTP handler for the POST /api/v2/things route.
func (h *ThingHandler) handlePostThing(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	req, err := decodePostThingRequest(ctx, r)
	if err != nil {
		EncodeError(ctx, err, w)
		return
	}

	// Do stuff here
	if err := h.ThingService.CreateThing(ctx, req.Thing); err != nil {
		EncodeError(ctx, err, w)
		return
	}

	if err := encodeResponse(ctx, w, http.StatusCreated, req.Thing); err != nil {
		h.Logger.Info("encoding response failed", zap.Error(err))
		return
	}
}
```

* `http.HandlerFunc`'s that require particular encoding of http responses should implement an encode response function
