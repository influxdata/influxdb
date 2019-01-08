package grpc

import (
	"fmt"
	"reflect"
	"testing"

	platform "github.com/influxdata/influxdb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToStatus(t *testing.T) {
	tests := []struct {
		name        string
		err         *platform.Error
		wantCode    codes.Code
		wantMessage string
		wantErr     bool
	}{
		{
			name:     "encode nil error",
			wantCode: codes.OK,
		},
		{
			name: "encode internal error",
			err: &platform.Error{
				Err:  fmt.Errorf("error"),
				Op:   "kit/grpc",
				Code: platform.EInternal,
				Msg:  "howdy",
			},
			wantCode:    codes.Internal,
			wantMessage: `{"code":"internal error","message":"howdy","op":"kit/grpc","error":"error"}`,
		},
		{
			name: "encode not found error",
			err: &platform.Error{
				Err:  fmt.Errorf("error"),
				Op:   "kit/grpc",
				Code: platform.ENotFound,
				Msg:  "howdy",
			},
			wantCode:    codes.NotFound,
			wantMessage: `{"code":"not found","message":"howdy","op":"kit/grpc","error":"error"}`,
		},
		{
			name: "encode invalid error",
			err: &platform.Error{
				Err:  fmt.Errorf("error"),
				Op:   "kit/grpc",
				Code: platform.EInvalid,
				Msg:  "howdy",
			},
			wantCode:    codes.InvalidArgument,
			wantMessage: `{"code":"invalid","message":"howdy","op":"kit/grpc","error":"error"}`,
		},
		{
			name: "encode unavailable error",
			err: &platform.Error{
				Err:  fmt.Errorf("error"),
				Op:   "kit/grpc",
				Code: platform.EUnavailable,
				Msg:  "howdy",
			},
			wantCode:    codes.Unavailable,
			wantMessage: `{"code":"unavailable","message":"howdy","op":"kit/grpc","error":"error"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToStatus(tt.err)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Code() != tt.wantCode {
				t.Errorf("ToStatus().Code() = %v, want %v", got.Code(), tt.wantCode)
			}
			if got.Message() != tt.wantMessage {
				t.Errorf("ToStatus().Message() = %v, want %v", got.Message(), tt.wantMessage)
			}
		})
	}
}

func TestFromStatus(t *testing.T) {
	tests := []struct {
		name string
		s    *status.Status
		want *platform.Error
	}{
		{
			name: "nil status returns nil error",
		},
		{
			name: "OK status returns nil error",
			s:    status.New(codes.OK, ""),
		},
		{
			name: "status message that is not JSON is internal error",
			s:    status.New(codes.Internal, "bad"),
			want: &platform.Error{
				Err:  fmt.Errorf("bad"),
				Code: platform.EInternal,
			},
		},
		{
			name: "status message with embedded platform error",
			s:    status.New(codes.Internal, `{"code":"unavailable","message":"howdy","op":"kit/grpc","error":"error"}`),
			want: &platform.Error{
				Err:  fmt.Errorf("error"),
				Code: platform.EUnavailable,
				Msg:  "howdy",
				Op:   "kit/grpc",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromStatus(tt.s); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}
