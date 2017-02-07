package bolt_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/influxdata/chronograf"
)

func setupTestClient() (*TestClient, error) {
	if c, err := NewTestClient(); err != nil {
		return nil, err
	} else if err := c.Open(); err != nil {
		return nil, err
	} else {
		return c, nil
	}
}

// Ensure an AlertRuleStore can be stored.
func TestAlertRuleStoreAdd(t *testing.T) {
	c, err := setupTestClient()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s := c.AlertsStore

	alerts := []chronograf.AlertRule{
		chronograf.AlertRule{
			ID: "one",
		},
		chronograf.AlertRule{
			ID:      "two",
			Details: "howdy",
		},
	}

	// Add new alert.
	ctx := context.Background()
	for i, a := range alerts {
		// Adding should return an identical copy
		actual, err := s.Add(ctx, 0, 0, a)
		if err != nil {
			t.Errorf("erroring adding alert to store: %v", err)
		}
		if !reflect.DeepEqual(actual, alerts[i]) {
			t.Fatalf("alert returned is different then alert saved; actual: %v, expected %v", actual, alerts[i])
		}
	}
}

func setupWithRule(ctx context.Context, alert chronograf.AlertRule) (*TestClient, error) {
	c, err := setupTestClient()
	if err != nil {
		return nil, err
	}

	// Add test alert
	if _, err := c.AlertsStore.Add(ctx, 0, 0, alert); err != nil {
		return nil, err
	}
	return c, nil
}

// Ensure an AlertRuleStore can be loaded.
func TestAlertRuleStoreGet(t *testing.T) {
	ctx := context.Background()
	alert := chronograf.AlertRule{
		ID: "one",
	}
	c, err := setupWithRule(ctx, alert)
	if err != nil {
		t.Fatalf("Error adding test alert to store: %v", err)
	}
	defer c.Close()
	actual, err := c.AlertsStore.Get(ctx, 0, 0, "one")
	if err != nil {
		t.Fatalf("Error loading rule from store: %v", err)
	}

	if !reflect.DeepEqual(actual, alert) {
		t.Fatalf("alert returned is different then alert saved; actual: %v, expected %v", actual, alert)
	}
}

// Ensure an AlertRuleStore can be load with a detail.
func TestAlertRuleStoreGetDetail(t *testing.T) {
	ctx := context.Background()
	alert := chronograf.AlertRule{
		ID:      "one",
		Details: "my details",
	}
	c, err := setupWithRule(ctx, alert)
	if err != nil {
		t.Fatalf("Error adding test alert to store: %v", err)
	}
	defer c.Close()
	actual, err := c.AlertsStore.Get(ctx, 0, 0, "one")
	if err != nil {
		t.Fatalf("Error loading rule from store: %v", err)
	}

	if !reflect.DeepEqual(actual, alert) {
		t.Fatalf("alert returned is different then alert saved; actual: %v, expected %v", actual, alert)
	}
}
