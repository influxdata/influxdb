// Package bolt provides an bolt-backed store implementation.
//
// The data stored in bolt is structured as follows:
//
//    bucket(/tasks/v1/tasks) key(:task_id) -> Content of submitted task (i.e. flux code).
//    bucket(/tasks/v1/task_meta) Key(:task_id) -> Protocol Buffer encoded pb.StoredTaskInternalMeta,
//                                    so we have a consistent view of runs in progress and max concurrency.
//    bucket(/tasks/v1/org_by_task_id) key(task_id) -> The organization ID (stored as encoded string) associated with given task.
//    bucket(/tasks/v1/user_by_task_id) key(:task_id) -> The user ID (stored as encoded string) associated with given task.
//    buket(/tasks/v1/name_by_task_id) key(:task_id) -> The user-supplied name of the script.
//                                         Maybe we don't need this after name becomes a script option?
//                                         Or maybe we do need it as part of ensuring uniqueness.
//    bucket(/tasks/v1/run_ids) -> Counter for run IDs
//    bucket(/tasks/v1/orgs).bucket(:org_id) key(:task_id) -> Empty content; presence of :task_id allows for lookup from org to tasks.
//    bucket(/tasks/v1/users).bucket(:user_id) key(:task_id) -> Empty content; presence of :task_id allows for lookup from user to tasks.
//
// Note that task IDs are stored big-endian uint64s for sorting purposes,
// but presented to the users with leading 0-bytes stripped.
// Like other components of the system, IDs presented to users may be `0f12` rather than `f12`.
package bolt

import (
	"context"
	"errors"
	"fmt"

	bolt "github.com/coreos/bbolt"
	"github.com/influxdata/platform"
	"github.com/influxdata/platform/task/backend"
	"github.com/influxdata/platform/task/backend/pb"
)

// ErrDBReadOnly is an error for when the database is set to read only.
// Tasks needs to be able to write to the db.
var ErrDBReadOnly = errors.New("db is read only")

// ErrMaxConcurrency is an error for when the max concurrency is already
// reached for a task when you try to schedule a task.
var ErrMaxConcurrency = errors.New("MaxConcurrency reached")

// ErrRunNotFound is an error for when a run isn't found in a FinishRun method.
var ErrRunNotFound = errors.New("run not found")

// ErrNotFound is an error for when a task could not be found
var ErrNotFound = errors.New("task not found")

// Store is task store for bolt.
type Store struct {
	db     *bolt.DB
	bucket []byte
}

const basePath = "/tasks/v1/"

var (
	tasksPath    = []byte(basePath + "tasks")
	orgsPath     = []byte(basePath + "orgs")
	usersPath    = []byte(basePath + "users")
	taskMetaPath = []byte(basePath + "task_meta")
	orgByTaskID  = []byte(basePath + "org_by_task_id")
	userByTaskID = []byte(basePath + "user_by_task_id")
	nameByTaskID = []byte(basePath + "name_by_task_id")
	runIDs       = []byte(basePath + "run_ids")
)

// New gives us a new Store based on "github.com/coreos/bbolt"
func New(db *bolt.DB, rootBucket string) (*Store, error) {
	if db.IsReadOnly() {
		return nil, ErrDBReadOnly
	}
	bucket := []byte(rootBucket)

	err := db.Update(func(tx *bolt.Tx) error {
		// create root
		root, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		// create the buckets inside the root
		for _, b := range [][]byte{
			tasksPath, orgsPath, usersPath, taskMetaPath,
			orgByTaskID, userByTaskID, nameByTaskID, runIDs,
		} {
			_, err := root.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Store{db: db, bucket: bucket}, nil
}

// CreateTask creates a task in the boltdb task store.
func (s *Store) CreateTask(ctx context.Context, org, user platform.ID, script string) (*platform.ID, error) {
	o, err := backend.StoreValidator.CreateArgs(org, user, script)
	if err != nil {
		return nil, err
	}

	encOrg, err := org.Encode()
	if err != nil {
		return nil, err
	}
	encUser, err := user.Encode()
	if err != nil {
		return nil, err
	}

	var id platform.ID
	err = s.db.Update(func(tx *bolt.Tx) error {
		// get the root bucket
		b := tx.Bucket(s.bucket)
		// Get ID
		idi, _ := b.NextSequence() // we ignore this err check, because this can't err inside an Update call
		id = platform.ID(idi)

		encID, err := id.Encode()
		if err != nil {
			return err
		}

		// write script
		err = b.Bucket(tasksPath).Put(encID, []byte(script))
		if err != nil {
			return err
		}

		// name
		err = b.Bucket(nameByTaskID).Put(encID, []byte(o.Name))
		if err != nil {
			return err
		}

		// org
		orgB, err := b.Bucket(orgsPath).CreateBucketIfNotExists(encOrg)
		if err != nil {
			return err
		}

		err = orgB.Put(encID, nil)
		if err != nil {
			return err
		}

		err = b.Bucket(orgByTaskID).Put(encID, encOrg)
		if err != nil {
			return err
		}

		// user
		userB, err := b.Bucket(usersPath).CreateBucketIfNotExists(encUser)
		if err != nil {
			return err
		}

		err = userB.Put(encID, nil)
		if err != nil {
			return err
		}

		err = b.Bucket(userByTaskID).Put(encID, encUser)
		if err != nil {
			return err
		}

		// metadata
		stm := pb.StoredTaskInternalMeta{
			MaxConcurrency: int32(o.Concurrency),
		}

		stmBytes, err := stm.Marshal()
		if err != nil {
			return err
		}
		metaB := b.Bucket(taskMetaPath)
		return metaB.Put(encID, stmBytes)
	})
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// ModifyTask changes a task with a new script, it should error if the task does not exist.
func (s *Store) ModifyTask(ctx context.Context, id platform.ID, newScript string) error {
	if _, err := backend.StoreValidator.ModifyArgs(id, newScript); err != nil {
		return err
	}

	encID, err := id.Encode()
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket).Bucket(tasksPath)
		if v := b.Get(encID); v == nil { // this is so we can error if the task doesn't exist
			return ErrNotFound
		}
		return b.Put(encID, []byte(newScript))
	})
}

// ListTasks lists the tasks based on a filter.
func (s *Store) ListTasks(ctx context.Context, params backend.TaskSearchParams) ([]backend.StoreTask, error) {
	if params.Org.Valid() && params.User.Valid() {
		return nil, errors.New("ListTasks: org and user filters are mutually exclusive")
	}

	const (
		defaultPageSize = 100
		maxPageSize     = 500
	)
	if params.PageSize < 0 {
		return nil, errors.New("ListTasks: PageSize must be positive")
	}
	if params.PageSize > maxPageSize {
		return nil, fmt.Errorf("ListTasks: PageSize exceeds maximum of %d", maxPageSize)
	}
	lim := params.PageSize
	if lim == 0 {
		lim = defaultPageSize
	}
	taskIDs := make([]platform.ID, 0, params.PageSize)

	err := s.db.View(func(tx *bolt.Tx) error {
		var c *bolt.Cursor
		b := tx.Bucket(s.bucket)
		if params.Org.Valid() {
			encOrgID, err := params.Org.Encode()
			if err != nil {
				return err
			}
			orgB := b.Bucket(orgsPath).Bucket(encOrgID)
			if orgB == nil {
				return ErrNotFound
			}
			c = orgB.Cursor()
		} else if params.User.Valid() {
			encUserID, err := params.User.Encode()
			if err != nil {
				return err
			}
			userB := b.Bucket(usersPath).Bucket(encUserID)
			if userB == nil {
				return ErrNotFound
			}
			c = userB.Cursor()
		} else {
			c = b.Bucket(tasksPath).Cursor()
		}
		if params.After.Valid() {
			encAfterID, err := params.After.Encode()
			if err != nil {
				return err
			}
			c.Seek(encAfterID)
			for k, _ := c.Next(); k != nil && len(taskIDs) < lim; k, _ = c.Next() {
				var id platform.ID
				if err := id.Decode(k); err != nil {
					return err
				}
				taskIDs = append(taskIDs, id)
			}
			return nil
		}
		for k, _ := c.First(); k != nil && len(taskIDs) < lim; k, _ = c.Next() {
			var id platform.ID
			if err := id.Decode(k); err != nil {
				return err
			}
			taskIDs = append(taskIDs, id)
		}
		return nil
	})
	if err == ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// now lookup each task
	tasks := make([]backend.StoreTask, len(taskIDs))
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		for i := range taskIDs {
			// TODO(docmerlin): optimization: don't check <-ctx.Done() every time though the loop
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// TODO(docmerlin): change the setup to reduce the number of lookups to 1 or 2.
				encID, err := taskIDs[i].Encode()
				if err != nil {
					return err
				}
				tasks[i].ID = taskIDs[i]
				tasks[i].Script = string(b.Bucket(tasksPath).Get(encID))
				tasks[i].Name = string(b.Bucket(nameByTaskID).Get(encID))
			}
		}
		if params.Org.Valid() {
			for i := range taskIDs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					encID, err := taskIDs[i].Encode()
					if err != nil {
						return err
					}
					tasks[i].Org = params.Org
					if err := tasks[i].User.Decode(b.Bucket(userByTaskID).Get(encID)); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if params.User.Valid() {
			for i := range taskIDs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					encID, err := taskIDs[i].Encode()
					if err != nil {
						return err
					}
					tasks[i].User = params.User
					if err := tasks[i].Org.Decode(b.Bucket(orgByTaskID).Get(encID)); err != nil {
						return err
					}
				}
			}
			return nil
		}
		for i := range taskIDs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				encID, err := taskIDs[i].Encode()
				if err != nil {
					return err
				}
				if err := tasks[i].User.Decode(b.Bucket(userByTaskID).Get(encID)); err != nil {
					return err
				}
				if err := tasks[i].Org.Decode(b.Bucket(orgByTaskID).Get(encID)); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return tasks, nil
}

// FindTaskByID finds a task with a given an ID.  It will return nil if the task does not exist.
func (s *Store) FindTaskByID(ctx context.Context, id platform.ID) (*backend.StoreTask, error) {
	var stmBytes []byte
	var script []byte
	var userID platform.ID
	var name []byte
	var orgID platform.ID
	encID, err := id.Encode()
	if err != nil {
		return nil, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		script = b.Bucket(tasksPath).Get(encID)
		if script == nil {
			return ErrNotFound
		}
		stmBytes = b.Bucket(taskMetaPath).Get(encID)
		if err := userID.Decode(b.Bucket(userByTaskID).Get(encID)); err != nil {
			return err
		}
		name = b.Bucket(nameByTaskID).Get(encID)
		if orgID.Decode(b.Bucket(orgByTaskID).Get(encID)); err != nil {
			return err
		}
		return nil
	})
	if err == ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	stm := pb.StoredTaskInternalMeta{}
	err = stm.Unmarshal(stmBytes)
	if err != nil {
		return nil, err
	}

	return &backend.StoreTask{
		ID:     id,
		Org:    orgID,
		User:   userID,
		Name:   string(name),
		Script: string(script),
	}, err
}

func (s *Store) FindTaskMetaByID(ctx context.Context, id platform.ID) (*pb.StoredTaskInternalMeta, error) {
	encID, err := id.Encode()
	if err != nil {
		return nil, err
	}
	var stmBytes []byte
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes = b.Bucket(taskMetaPath).Get(encID)
		if stmBytes == nil {
			return errors.New("task meta not found")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	stm := pb.StoredTaskInternalMeta{}
	err = stm.Unmarshal(stmBytes)
	if err != nil {
		return nil, err
	}

	return &stm, nil
}

// DeleteTask deletes the task
func (s *Store) DeleteTask(ctx context.Context, id platform.ID) (deleted bool, err error) {
	encID, err := id.Encode()
	if err != nil {
		return false, err
	}
	err = s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		if check := b.Bucket(tasksPath).Get(encID); check == nil {
			return ErrNotFound
		}
		if err := b.Bucket(taskMetaPath).Delete(encID); err != nil {
			return err
		}
		if err := b.Bucket(tasksPath).Delete(encID); err != nil {
			return err
		}
		user := b.Bucket(userByTaskID).Get(encID)
		if len(user) > 0 {
			if err := b.Bucket(usersPath).Bucket(user).Delete(encID); err != nil {
				return err
			}
		}
		if err := b.Bucket(userByTaskID).Delete(encID); err != nil {
			return err
		}
		if err := b.Bucket(nameByTaskID).Delete(encID); err != nil {
			return err
		}

		org := b.Bucket(orgByTaskID).Get(encID)
		if len(org) > 0 {
			if err := b.Bucket(orgsPath).Bucket(org).Delete(encID); err != nil {
				return err
			}
		}
		return b.Bucket(orgByTaskID).Delete(encID)
	})
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// CreateRun adds `now` to the task's metaData if we have not exceeded 'max_concurrency'.
func (s *Store) CreateRun(ctx context.Context, taskID platform.ID, now int64) (backend.QueuedRun, error) {
	queuedRun := backend.QueuedRun{TaskID: taskID, Now: now}
	stm := pb.StoredTaskInternalMeta{}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		encID, err := taskID.Encode()
		if err != nil {
			return err
		}

		b := tx.Bucket(s.bucket)
		stmBytes := b.Bucket(taskMetaPath).Get(encID)
		if err := stm.Unmarshal(stmBytes); err != nil {
			return err
		}
		if len(stm.CurrentlyRunning) >= int(stm.MaxConcurrency) {
			return ErrMaxConcurrency
		}
		intID, err := b.Bucket(runIDs).NextSequence()
		if err != nil {
			return err
		}

		running := &pb.StoredTaskInternalMeta_RunningList{
			NowTimestampUnix: now,
			Try:              1,
			RunID:            intID,
		}

		stm.CurrentlyRunning = append(stm.CurrentlyRunning, running)
		stmBytes, err = stm.Marshal()
		if err != nil {
			return err
		}

		queuedRun.RunID = platform.ID(intID)

		return tx.Bucket(s.bucket).Bucket(taskMetaPath).Put(encID, stmBytes)
	}); err != nil {
		return queuedRun, err
	}

	return queuedRun, nil
}

// FinishRun removes runID from the list of running tasks and if its `now` is later then last completed update it.
func (s *Store) FinishRun(ctx context.Context, taskID, runID platform.ID) error {
	stm := pb.StoredTaskInternalMeta{}
	encID, err := taskID.Encode()
	if err != nil {
		return err
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)
		stmBytes := b.Bucket(taskMetaPath).Get(encID)
		if err := stm.Unmarshal(stmBytes); err != nil {
			return err
		}
		found := false
		for i, runner := range stm.CurrentlyRunning {
			if platform.ID(runner.RunID) == runID {
				found = true
				stm.CurrentlyRunning = append(stm.CurrentlyRunning[:i], stm.CurrentlyRunning[i+1:]...)
				if runner.NowTimestampUnix > stm.LastCompletedTimestampUnix {
					stm.LastCompletedTimestampUnix = runner.NowTimestampUnix
					break
				}
			}
		}
		if !found {
			return ErrRunNotFound
		}

		stmBytes, err := stm.Marshal()
		if err != nil {
			return err
		}

		return tx.Bucket(s.bucket).Bucket(taskMetaPath).Put(encID, stmBytes)
	})
}

// Close closes the store
func (s *Store) Close() error {
	return s.db.Close()
}

// DeleteUser syncronously deletes a user and all their tasks from a bolt store.
func (s *Store) DeleteUser(ctx context.Context, id platform.ID) error {
	encUserID, err := id.Encode()
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)

		ub := b.Bucket(usersPath).Bucket(encUserID)
		if ub == nil {
			return backend.ErrUserNotFound
		}
		c := ub.Cursor()
		i := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
			// check for cancelation every 256 tasks deleted
			if i&0xFF == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			if err := b.Bucket(tasksPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(taskMetaPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(orgByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(userByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(nameByTaskID).Delete(k); err != nil {
				return err
			}

			org := b.Bucket(orgByTaskID).Get(k)
			if len(org) > 0 {
				ob := b.Bucket(orgsPath).Bucket(org)
				if ob != nil {
					if err := ob.Delete(k); err != nil {
						return err
					}
				}
			}
		}

		// check for cancelation one last time before we return
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return b.Bucket(usersPath).DeleteBucket(encUserID)
		}
	})

	return err
}

// DeleteOrg syncronously deletes an org and all their tasks from a bolt store.
func (s *Store) DeleteOrg(ctx context.Context, id platform.ID) error {
	encOrgID, err := id.Encode()
	if err != nil {
		return err
	}

	return s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucket)

		ob := b.Bucket(orgsPath).Bucket(encOrgID)
		if ob == nil {
			return backend.ErrOrgNotFound
		}
		c := ob.Cursor()
		i := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			i++
			// check for cancelation every 256 tasks deleted
			if i&0xFF == 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			if err := b.Bucket(tasksPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(taskMetaPath).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(orgByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(userByTaskID).Delete(k); err != nil {
				return err
			}
			if err := b.Bucket(nameByTaskID).Delete(k); err != nil {
				return err
			}
			user := b.Bucket(userByTaskID).Get(k)
			if len(user) > 0 {
				ub := b.Bucket(usersPath).Bucket(user)
				if ub != nil {
					if err := ub.Delete(k); err != nil {
						return err
					}
				}
			}
		}
		// check for cancelation one last time before we return
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return b.Bucket(orgsPath).DeleteBucket(encOrgID)
		}
	})
}
