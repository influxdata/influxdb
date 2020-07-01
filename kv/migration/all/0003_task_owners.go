package all

import (
	"context"
	"encoding/json"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/kv"
)

var taskBucket = []byte("tasksv1")

// Migration0003_TaskOwnerIDUpMigration adds missing owner IDs to some legacy tasks
var Migration0003_TaskOwnerIDUpMigration = UpOnlyMigration(
	"migrate task owner id",
	func(ctx context.Context, store kv.SchemaStore) error {
		var ownerlessTasks []*influxdb.Task
		// loop through the tasks and collect a set of tasks that are missing the owner id.
		err := store.View(ctx, func(tx kv.Tx) error {
			taskBucket, err := tx.Bucket(taskBucket)
			if err != nil {
				return influxdb.ErrUnexpectedTaskBucketErr(err)
			}

			c, err := taskBucket.ForwardCursor([]byte{})
			if err != nil {
				return influxdb.ErrUnexpectedTaskBucketErr(err)
			}

			for k, v := c.Next(); k != nil; k, v = c.Next() {
				kvTask := &kvTask{}
				if err := json.Unmarshal(v, kvTask); err != nil {
					return influxdb.ErrInternalTaskServiceError(err)
				}

				t := kvToInfluxTask(kvTask)

				if !t.OwnerID.Valid() {
					ownerlessTasks = append(ownerlessTasks, t)
				}
			}
			if err := c.Err(); err != nil {
				return err
			}

			return c.Close()
		})
		if err != nil {
			return err
		}

		// loop through tasks
		for _, t := range ownerlessTasks {
			// open transaction
			err := store.Update(ctx, func(tx kv.Tx) error {
				taskKey, err := taskKey(t.ID)
				if err != nil {
					return err
				}
				b, err := tx.Bucket(taskBucket)
				if err != nil {
					return influxdb.ErrUnexpectedTaskBucketErr(err)
				}

				if !t.OwnerID.Valid() {
					v, err := b.Get(taskKey)
					if kv.IsNotFound(err) {
						return influxdb.ErrTaskNotFound
					}
					authType := struct {
						AuthorizationID influxdb.ID `json:"authorizationID"`
					}{}
					if err := json.Unmarshal(v, &authType); err != nil {
						return influxdb.ErrInternalTaskServiceError(err)
					}

					// try populating the owner from auth
					encodedID, err := authType.AuthorizationID.Encode()
					if err == nil {
						authBucket, err := tx.Bucket([]byte("authorizationsv1"))
						if err != nil {
							return err
						}

						a, err := authBucket.Get(encodedID)
						if err == nil {
							auth := &influxdb.Authorization{}
							if err := json.Unmarshal(a, auth); err != nil {
								return err
							}

							t.OwnerID = auth.GetUserID()
						}
					}

				}

				// try populating owner from urm
				if !t.OwnerID.Valid() {
					b, err := tx.Bucket([]byte("userresourcemappingsv1"))
					if err != nil {
						return err
					}

					id, err := t.OrganizationID.Encode()
					if err != nil {
						return err
					}

					cur, err := b.ForwardCursor(id, kv.WithCursorPrefix(id))
					if err != nil {
						return err
					}

					for k, v := cur.Next(); k != nil; k, v = cur.Next() {
						m := &influxdb.UserResourceMapping{}
						if err := json.Unmarshal(v, m); err != nil {
							return err
						}
						if m.ResourceID == t.OrganizationID && m.ResourceType == influxdb.OrgsResourceType && m.UserType == influxdb.Owner {
							t.OwnerID = m.UserID
							break
						}
					}

					if err := cur.Close(); err != nil {
						return err
					}
				}

				// if population fails return error
				if !t.OwnerID.Valid() {
					return &influxdb.Error{
						Code: influxdb.EInternal,
						Msg:  "could not populate owner ID for task",
					}
				}

				// save task
				taskBytes, err := json.Marshal(t)
				if err != nil {
					return influxdb.ErrInternalTaskServiceError(err)
				}

				err = b.Put(taskKey, taskBytes)
				if err != nil {
					return influxdb.ErrUnexpectedTaskBucketErr(err)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	},
)

type kvTask struct {
	ID              influxdb.ID            `json:"id"`
	Type            string                 `json:"type,omitempty"`
	OrganizationID  influxdb.ID            `json:"orgID"`
	Organization    string                 `json:"org"`
	OwnerID         influxdb.ID            `json:"ownerID"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description,omitempty"`
	Status          string                 `json:"status"`
	Flux            string                 `json:"flux"`
	Every           string                 `json:"every,omitempty"`
	Cron            string                 `json:"cron,omitempty"`
	LastRunStatus   string                 `json:"lastRunStatus,omitempty"`
	LastRunError    string                 `json:"lastRunError,omitempty"`
	Offset          influxdb.Duration      `json:"offset,omitempty"`
	LatestCompleted time.Time              `json:"latestCompleted,omitempty"`
	LatestScheduled time.Time              `json:"latestScheduled,omitempty"`
	CreatedAt       time.Time              `json:"createdAt,omitempty"`
	UpdatedAt       time.Time              `json:"updatedAt,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

func kvToInfluxTask(k *kvTask) *influxdb.Task {
	return &influxdb.Task{
		ID:              k.ID,
		Type:            k.Type,
		OrganizationID:  k.OrganizationID,
		Organization:    k.Organization,
		OwnerID:         k.OwnerID,
		Name:            k.Name,
		Description:     k.Description,
		Status:          k.Status,
		Flux:            k.Flux,
		Every:           k.Every,
		Cron:            k.Cron,
		LastRunStatus:   k.LastRunStatus,
		LastRunError:    k.LastRunError,
		Offset:          k.Offset.Duration,
		LatestCompleted: k.LatestCompleted,
		LatestScheduled: k.LatestScheduled,
		CreatedAt:       k.CreatedAt,
		UpdatedAt:       k.UpdatedAt,
		Metadata:        k.Metadata,
	}
}

func taskKey(taskID influxdb.ID) ([]byte, error) {
	encodedID, err := taskID.Encode()
	if err != nil {
		return nil, influxdb.ErrInvalidTaskID
	}
	return encodedID, nil
}
