package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/cmd/influx/internal"
)

func membersListF(ctx context.Context, filter influxdb.UserResourceMappingFilter) error {
	mappingSvc, err := newUserResourceMappingService()
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
	}
	mps, _, err := mappingSvc.FindUserResourceMappings(ctx, filter)
	if err != nil {
		return fmt.Errorf("failed to find members: %v", err)
	}
	userSVC, err := newUserService()
	if err != nil {
		return fmt.Errorf("failed to initialize users service client: %v", err)
	}
	urs := make([]*influxdb.User, len(mps))
	ursC := make(chan struct {
		User  *influxdb.User
		Index int
	})
	errC := make(chan error)
	sem := make(chan struct{}, maxTCPConnections)
	for k, v := range mps {
		sem <- struct{}{}
		go func(k int, v *influxdb.UserResourceMapping) {
			defer func() { <-sem }()
			usr, err := userSVC.FindUserByID(ctx, v.UserID)
			if err != nil {
				errC <- fmt.Errorf("failed to retrieve user details: %v", err)
				return
			}
			ursC <- struct {
				User  *influxdb.User
				Index int
			}{
				User:  usr,
				Index: k,
			}
		}(k, v)
	}
	for i := 0; i < len(mps); i++ {
		select {
		case <-ctx.Done():
			return &influxdb.Error{
				Msg: "Timeout retrieving user details",
			}
		case err := <-errC:
			return err
		case item := <-ursC:
			urs[item.Index] = item.User
		}
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"Status",
	)
	for _, m := range urs {
		w.Write(map[string]interface{}{
			"ID":     m.ID.String(),
			"Name":   m.Name,
			"Status": string(m.Status),
		})
	}
	w.Flush()
	return nil
}

func membersAddF(ctx context.Context, urm influxdb.UserResourceMapping) error {
	mappingSvc, err := newUserResourceMappingService()
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
	}

	if err = mappingSvc.CreateUserResourceMapping(ctx, &urm); err != nil {
		return fmt.Errorf("failed to add member: %v", err)
	}
	fmt.Printf("user %s has been added as a %s of %s: %s\n", urm.UserID, urm.UserType, urm.ResourceType, urm.ResourceID)
	return nil
}

func membersRemoveF(ctx context.Context, resourceID, userID influxdb.ID) error {
	mappingSvc, err := newUserResourceMappingService()
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
	}
	if err = mappingSvc.DeleteUserResourceMapping(ctx, resourceID, userID); err != nil {
		return fmt.Errorf("failed to remove member: %v", err)
	}
	fmt.Printf("userID %s has been removed from ResourceID %s\n", userID, resourceID)
	return nil
}
