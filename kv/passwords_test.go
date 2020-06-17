package kv_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
	"github.com/influxdata/influxdb/v2/mock"
	influxdbtesting "github.com/influxdata/influxdb/v2/testing"
	"go.uber.org/zap/zaptest"
)

func TestBoltPasswordService(t *testing.T) {
	influxdbtesting.PasswordsService(initBoltPasswordsService, t)
}

func initBoltPasswordsService(f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	s, closeStore, err := NewTestBoltStore(t)
	if err != nil {
		t.Fatalf("failed to create new bolt kv store: %v", err)
	}

	svc, closeSvc := initPasswordsService(s, f, t)
	return svc, func() {
		closeSvc()
		closeStore()
	}
}

func initPasswordsService(s kv.SchemaStore, f influxdbtesting.PasswordFields, t *testing.T) (influxdb.PasswordsService, func()) {
	ctx := context.Background()
	svc := kv.NewService(zaptest.NewLogger(t), s)

	svc.IDGenerator = f.IDGenerator

	for _, u := range f.Users {
		if err := svc.PutUser(ctx, u); err != nil {
			t.Fatalf("error populating users: %v", err)
		}
	}

	for i := range f.Passwords {
		if err := svc.SetPassword(ctx, f.Users[i].ID, f.Passwords[i]); err != nil {
			t.Fatalf("error setting passsword user, %s %s: %v", f.Users[i].Name, f.Passwords[i], err)
		}
	}

	return svc, func() {
		for _, u := range f.Users {
			if err := svc.DeleteUser(ctx, u.ID); err != nil {
				t.Logf("error removing users: %v", err)
			}
		}
	}
}

type MockHasher struct {
	GenerateError error
	CompareError  error
}

func (m *MockHasher) CompareHashAndPassword(hashedPassword, password []byte) error {
	return m.CompareError
}

func (m *MockHasher) GenerateFromPassword(password []byte, cost int) ([]byte, error) {
	return nil, m.GenerateError
}

func TestService_SetPassword(t *testing.T) {
	type fields struct {
		kv   kv.Store
		Hash kv.Crypt
	}
	type args struct {
		id       influxdb.ID
		password string
	}
	type wants struct {
		err error
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "if store somehow has a corrupted user index, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										return nil, errors.New("its broked")
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("your userID is incorrect"),
			},
		},
		{
			name: "if user id is not found return a generic sounding error",
			fields: fields{
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										return nil, kv.ErrKeyNotFound
									},
									PutFn: func(key, val []byte) error {
										return nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("your userID is incorrect"),
			},
		},
		{
			name: "if store somehow has a corrupted user id, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "0000000000000001" {
											return []byte(`{"name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
									PutFn: func(key, val []byte) error {
										return nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       0,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("User ID  has been corrupted; Err: invalid ID"),
			},
		},
		{
			name: "if password store is not available, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								if string(b) == "userspasswordv1" {
									return nil, fmt.Errorf("internal bucket error")
								}
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "0000000000000001" {
											return []byte(`{"id": "0000000000000001", "name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
									PutFn: func(key, val []byte) error {
										return nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("Unable to connect to password service. Please try again; Err: internal bucket error"),
			},
		},
		{
			name: "if hashing algorithm has an error, then, we get back an internal error",
			fields: fields{
				Hash: &MockHasher{
					GenerateError: fmt.Errorf("generate error"),
				},
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								if string(b) == "userspasswordv1" {
									return nil, nil
								}
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "0000000000000001" {
											return []byte(`{"id": "0000000000000001", "name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
									PutFn: func(key, val []byte) error {
										return nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				fmt.Errorf("Unable to generate password; Err: generate error"),
			},
		},
		{
			name: "if not able to store the hashed password should have an internal error",
			fields: fields{
				kv: &mock.Store{
					UpdateFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								if string(b) == "userspasswordv1" {
									return &mock.Bucket{
										PutFn: func(key, value []byte) error {
											return fmt.Errorf("internal error")
										},
									}, nil
								}
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "0000000000000001" {
											return []byte(`{"id": "0000000000000001", "name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
									PutFn: func(key, val []byte) error {
										return nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				fmt.Errorf("Unable to connect to password service. Please try again; Err: internal error"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &kv.Service{
				Hash: tt.fields.Hash,
			}
			s.WithStore(tt.fields.kv)

			err := s.SetPassword(context.Background(), tt.args.id, tt.args.password)
			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("Service.SetPassword() error = %v, want %v", err, tt.wants.err)
				return
			}

			if err != nil {
				if got, want := err.Error(), tt.wants.err.Error(); got != want {
					t.Errorf("Service.SetPassword() error = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestService_ComparePassword(t *testing.T) {
	type fields struct {
		kv   kv.Store
		Hash kv.Crypt
	}
	type args struct {
		id       influxdb.ID
		password string
	}
	type wants struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		wants  wants
	}{
		{
			name: "if store somehow has a corrupted user index, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					ViewFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										return nil, nil
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("your userID is incorrect"),
			},
		},
		{
			name: "if store somehow has a corrupted user id, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					ViewFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "user1" {
											return []byte("0000000000000001"), nil
										}
										if string(key) == "0000000000000001" {
											return []byte(`{"name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       0,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("User ID  has been corrupted; Err: invalid ID"),
			},
		},
		{
			name: "if password store is not available, then, we get back an internal error",
			fields: fields{
				kv: &mock.Store{
					ViewFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								if string(b) == "userspasswordv1" {
									return nil, fmt.Errorf("internal bucket error")
								}
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "user1" {
											return []byte("0000000000000001"), nil
										}
										if string(key) == "0000000000000001" {
											return []byte(`{"id": "0000000000000001", "name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				err: fmt.Errorf("Unable to connect to password service. Please try again; Err: internal bucket error"),
			},
		},
		{
			name: "if the password doesn't has correctly we get an invalid password error",
			fields: fields{
				Hash: &MockHasher{
					CompareError: fmt.Errorf("generate error"),
				},
				kv: &mock.Store{
					ViewFn: func(fn func(kv.Tx) error) error {
						tx := &mock.Tx{
							BucketFn: func(b []byte) (kv.Bucket, error) {
								if string(b) == "userspasswordv1" {
									return &mock.Bucket{
										GetFn: func([]byte) ([]byte, error) {
											return []byte("hash"), nil
										},
									}, nil
								}
								return &mock.Bucket{
									GetFn: func(key []byte) ([]byte, error) {
										if string(key) == "user1" {
											return []byte("0000000000000001"), nil
										}
										if string(key) == "0000000000000001" {
											return []byte(`{"id": "0000000000000001", "name": "user1"}`), nil
										}
										return nil, kv.ErrKeyNotFound
									},
								}, nil
							},
						}
						return fn(tx)
					},
				},
			},
			args: args{
				id:       1,
				password: "howdydoody",
			},
			wants: wants{
				fmt.Errorf("your username or password is incorrect"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &kv.Service{
				Hash: tt.fields.Hash,
			}
			s.WithStore(tt.fields.kv)
			err := s.ComparePassword(context.Background(), tt.args.id, tt.args.password)

			if (err != nil && tt.wants.err == nil) || (err == nil && tt.wants.err != nil) {
				t.Fatalf("Service.ComparePassword() error = %v, want %v", err, tt.wants.err)
				return
			}

			if err != nil {
				if got, want := err.Error(), tt.wants.err.Error(); got != want {
					t.Errorf("Service.ComparePassword() error = %v, want %v", got, want)
				}
			}
		})
	}
}
