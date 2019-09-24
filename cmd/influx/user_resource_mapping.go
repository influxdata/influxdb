package main

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb"
)

func membersAddF(urm influxdb.UserResourceMapping, flgs Flags) error {
	mappingSvc, err := newUserResourceMappingService(flgs)
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
	}

	if err = mappingSvc.CreateUserResourceMapping(context.Background(), &urm); err != nil {
		return fmt.Errorf("failed to add member: %v", err)
	}
	fmt.Printf("user %s has been added as a %s of %s: %s\n", urm.UserID, urm.UserType, urm.ResourceType, urm.ResourceID)
	return nil
}

func membersRemoveF(resourceID, userID influxdb.ID, flgs Flags) error {
	mappingSvc, err := newUserResourceMappingService(flgs)
	if err != nil {
		return fmt.Errorf("failed to initialize members service client: %v", err)
	}
	if err = mappingSvc.DeleteUserResourceMapping(context.Background(), resourceID, userID); err != nil {
		return fmt.Errorf("failed to remove member: %v", err)
	}
	fmt.Printf("userID %s has been removed from ResourceID %s\n", userID, resourceID)
	return nil
}
