// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/influxdata/influxdb/v2 (interfaces: PasswordsService)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	"github.com/influxdata/influxdb/v2/kit/platform"
	reflect "reflect"
)

// MockPasswordsService is a mock of PasswordsService interface
type MockPasswordsService struct {
	ctrl     *gomock.Controller
	recorder *MockPasswordsServiceMockRecorder
}

// MockPasswordsServiceMockRecorder is the mock recorder for MockPasswordsService
type MockPasswordsServiceMockRecorder struct {
	mock *MockPasswordsService
}

// NewMockPasswordsService creates a new mock instance
func NewMockPasswordsService(ctrl *gomock.Controller) *MockPasswordsService {
	mock := &MockPasswordsService{ctrl: ctrl}
	mock.recorder = &MockPasswordsServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockPasswordsService) EXPECT() *MockPasswordsServiceMockRecorder {
	return m.recorder
}

// CompareAndSetPassword mocks base method
func (m *MockPasswordsService) CompareAndSetPassword(arg0 context.Context, arg1 platform.ID, arg2, arg3 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CompareAndSetPassword", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// CompareAndSetPassword indicates an expected call of CompareAndSetPassword
func (mr *MockPasswordsServiceMockRecorder) CompareAndSetPassword(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CompareAndSetPassword", reflect.TypeOf((*MockPasswordsService)(nil).CompareAndSetPassword), arg0, arg1, arg2, arg3)
}

// ComparePassword mocks base method
func (m *MockPasswordsService) ComparePassword(arg0 context.Context, arg1 platform.ID, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComparePassword", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// ComparePassword indicates an expected call of ComparePassword
func (mr *MockPasswordsServiceMockRecorder) ComparePassword(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ComparePassword", reflect.TypeOf((*MockPasswordsService)(nil).ComparePassword), arg0, arg1, arg2)
}

// SetPassword mocks base method
func (m *MockPasswordsService) SetPassword(arg0 context.Context, arg1 platform.ID, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetPassword", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetPassword indicates an expected call of SetPassword
func (mr *MockPasswordsServiceMockRecorder) SetPassword(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetPassword", reflect.TypeOf((*MockPasswordsService)(nil).SetPassword), arg0, arg1, arg2)
}
