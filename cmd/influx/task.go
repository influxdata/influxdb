package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/influxdata/flux/repl"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

func cmdTask() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task",
		Short: "Task management commands",
		RunE: wrapCheckSetup(func(cmd *cobra.Command, args []string) error {
			if flags.local {
				return fmt.Errorf("local flag not supported for task command")
			}

			seeHelp(cmd, args)
			return nil
		}),
	}

	cmd.AddCommand(
		taskLogCmd(),
		taskRunCmd(),
		taskCreateCmd(),
		taskDeleteCmd(),
		taskFindCmd(),
		taskUpdateCmd(),
	)

	return cmd
}

var taskCreateFlags struct {
	org organization
}

func taskCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create [query literal or @/path/to/query.flux]",
		Short: "Create task",
		Args:  cobra.ExactArgs(1),
		RunE:  wrapCheckSetup(taskCreateF),
	}

	taskCreateFlags.org.register(cmd, false)

	return cmd
}

func taskCreateF(cmd *cobra.Command, args []string) error {
	if err := taskCreateFlags.org.validOrgFlags(); err != nil {
		return err
	}

	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	flux, err := repl.LoadQuery(args[0])
	if err != nil {
		return fmt.Errorf("error parsing flux script: %s", err)
	}

	tc := platform.TaskCreate{
		Flux:         flux,
		Organization: taskCreateFlags.org.name,
	}
	if taskCreateFlags.org.id != "" || taskCreateFlags.org.name != "" {
		svc, err := newOrganizationService()
		if err != nil {
			return nil
		}
		oid, err := taskCreateFlags.org.getID(svc)
		if err != nil {
			return fmt.Errorf("error parsing organization ID: %s", err)
		}
		tc.OrganizationID = oid
	}

	t, err := s.CreateTask(context.Background(), tc)
	if err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
		"AuthorizationID",
		"Status",
		"Every",
		"Cron",
	)
	w.Write(map[string]interface{}{
		"ID":             t.ID.String(),
		"Name":           t.Name,
		"OrganizationID": t.OrganizationID.String(),
		"Organization":   t.Organization,
		"Status":         t.Status,
		"Every":          t.Every,
		"Cron":           t.Cron,
	})
	w.Flush()

	return nil
}

var taskFindFlags struct {
	user    string
	id      string
	limit   int
	headers bool
	org     organization
}

func taskFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "Find tasks",
		RunE:  wrapCheckSetup(taskFindF),
	}

	taskFindFlags.org.register(cmd, false)
	cmd.Flags().StringVarP(&taskFindFlags.id, "id", "i", "", "task ID")
	cmd.Flags().StringVarP(&taskFindFlags.user, "user-id", "n", "", "task owner ID")
	cmd.Flags().IntVarP(&taskFindFlags.limit, "limit", "", platform.TaskDefaultPageSize, "the number of tasks to find")
	cmd.Flags().BoolVar(&taskFindFlags.headers, "headers", true, "To print the table headers; defaults true")

	return cmd
}

func taskFindF(cmd *cobra.Command, args []string) error {
	if err := taskFindFlags.org.validOrgFlags(); err != nil {
		return err
	}
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	filter := platform.TaskFilter{}
	if taskFindFlags.user != "" {
		id, err := platform.IDFromString(taskFindFlags.user)
		if err != nil {
			return err
		}
		filter.User = id
	}

	if taskFindFlags.org.name != "" {
		filter.Organization = taskFindFlags.org.name
	}
	if taskFindFlags.org.id != "" {
		id, err := platform.IDFromString(taskFindFlags.org.id)
		if err != nil {
			return err
		}
		filter.OrganizationID = id
	}

	if taskFindFlags.limit < 1 || taskFindFlags.limit > platform.TaskMaxPageSize {
		return fmt.Errorf("limit must be between 1 and %d", platform.TaskMaxPageSize)
	}
	filter.Limit = taskFindFlags.limit

	var tasks []http.Task
	var err error

	if taskFindFlags.id != "" {
		id, err := platform.IDFromString(taskFindFlags.id)
		if err != nil {
			return err
		}

		task, err := s.FindTaskByID(context.Background(), *id)
		if err != nil {
			return err
		}

		tasks = append(tasks, *task)
	} else {
		tasks, _, err = s.FindTasks(context.Background(), filter)
		if err != nil {
			return err
		}
	}

	w := internal.NewTabWriter(os.Stdout)
	w.HideHeaders(!taskFindFlags.headers)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
		"AuthorizationID",
		"Status",
		"Every",
		"Cron",
	)
	for _, t := range tasks {
		w.Write(map[string]interface{}{
			"ID":             t.ID.String(),
			"Name":           t.Name,
			"OrganizationID": t.OrganizationID.String(),
			"Organization":   t.Organization,
			"Status":         t.Status,
			"Every":          t.Every,
			"Cron":           t.Cron,
		})
	}
	w.Flush()

	return nil
}

var taskUpdateFlags struct {
	id     string
	status string
}

func taskUpdateCmd() *cobra.Command {
	taskUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update task",
		RunE:  wrapCheckSetup(taskUpdateF),
	}

	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.id, "id", "i", "", "task ID (required)")
	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.status, "status", "", "", "update task status")
	taskUpdateCmd.MarkFlagRequired("id")

	return taskUpdateCmd
}

func taskUpdateF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	var id platform.ID
	if err := id.DecodeFromString(taskUpdateFlags.id); err != nil {
		return err
	}

	update := platform.TaskUpdate{}
	if taskUpdateFlags.status != "" {
		update.Status = &taskUpdateFlags.status
	}

	if len(args) > 0 {
		flux, err := repl.LoadQuery(args[0])
		if err != nil {
			return fmt.Errorf("error parsing flux script: %s", err)
		}
		update.Flux = &flux
	}

	t, err := s.UpdateTask(context.Background(), id, update)
	if err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
		"AuthorizationID",
		"Status",
		"Every",
		"Cron",
	)
	w.Write(map[string]interface{}{
		"ID":             t.ID.String(),
		"Name":           t.Name,
		"OrganizationID": t.OrganizationID.String(),
		"Organization":   t.Organization,
		"Status":         t.Status,
		"Every":          t.Every,
		"Cron":           t.Cron,
	})
	w.Flush()

	return nil
}

var taskDeleteFlags struct {
	id string
}

func taskDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete task",
		RunE:  wrapCheckSetup(taskDeleteF),
	}

	cmd.Flags().StringVarP(&taskDeleteFlags.id, "id", "i", "", "task id (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func taskDeleteF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	var id platform.ID
	err := id.DecodeFromString(taskDeleteFlags.id)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	t, err := s.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	if err = s.DeleteTask(ctx, id); err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
		"AuthorizationID",
		"Status",
		"Every",
		"Cron",
	)
	w.Write(map[string]interface{}{
		"ID":             t.ID.String(),
		"Name":           t.Name,
		"OrganizationID": t.OrganizationID.String(),
		"Organization":   t.Organization,
		"Status":         t.Status,
		"Every":          t.Every,
		"Cron":           t.Cron,
	})
	w.Flush()

	return nil
}

func taskLogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log",
		Short: "Log related commands",
		Run:   seeHelp,
	}

	cmd.AddCommand(
		taskLogFindCmd(),
	)

	return cmd
}

var taskLogFindFlags struct {
	taskID string
	runID  string
}

func taskLogFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "find logs for task",
		RunE:  wrapCheckSetup(taskLogFindF),
	}

	cmd.Flags().StringVarP(&taskLogFindFlags.taskID, "task-id", "", "", "task id (required)")
	cmd.Flags().StringVarP(&taskLogFindFlags.runID, "run-id", "", "", "run id")
	cmd.MarkFlagRequired("task-id")

	return cmd
}

func taskLogFindF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	var filter platform.LogFilter
	id, err := platform.IDFromString(taskLogFindFlags.taskID)
	if err != nil {
		return err
	}
	filter.Task = *id

	if taskLogFindFlags.runID != "" {
		id, err := platform.IDFromString(taskLogFindFlags.runID)
		if err != nil {
			return err
		}
		filter.Run = id
	}

	ctx := context.TODO()
	logs, _, err := s.FindLogs(ctx, filter)
	if err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"RunID",
		"Time",
		"Message",
	)
	for _, log := range logs {
		w.Write(map[string]interface{}{
			"RunID":   log.RunID,
			"Time":    log.Time,
			"Message": log.Message,
		})
	}
	w.Flush()

	return nil
}

func taskRunCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run related commands",
		Run:   seeHelp,
	}
	cmd.AddCommand(
		taskRunFindCmd(),
		taskRunRetryCmd(),
	)

	return cmd
}

var taskRunFindFlags struct {
	runID      string
	taskID     string
	afterTime  string
	beforeTime string
	limit      int
}

func taskRunFindCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "find",
		Short: "find runs for a task",
		RunE:  wrapCheckSetup(taskRunFindF),
	}

	cmd.Flags().StringVarP(&taskRunFindFlags.taskID, "task-id", "", "", "task id (required)")
	cmd.Flags().StringVarP(&taskRunFindFlags.runID, "run-id", "", "", "run id")
	cmd.Flags().StringVarP(&taskRunFindFlags.afterTime, "after", "", "", "after time for filtering")
	cmd.Flags().StringVarP(&taskRunFindFlags.beforeTime, "before", "", "", "before time for filtering")
	cmd.Flags().IntVarP(&taskRunFindFlags.limit, "limit", "", 0, "limit the results")

	cmd.MarkFlagRequired("task-id")

	return cmd
}

func taskRunFindF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	filter := platform.RunFilter{
		Limit:      taskRunFindFlags.limit,
		AfterTime:  taskRunFindFlags.afterTime,
		BeforeTime: taskRunFindFlags.beforeTime,
	}
	taskID, err := platform.IDFromString(taskRunFindFlags.taskID)
	if err != nil {
		return err
	}
	filter.Task = *taskID

	var runs []*platform.Run
	if taskRunFindFlags.runID != "" {
		id, err := platform.IDFromString(taskRunFindFlags.runID)
		if err != nil {
			return err
		}
		run, err := s.FindRunByID(context.Background(), filter.Task, *id)
		if err != nil {
			return err
		}
		runs = append(runs, run)
	} else {
		runs, _, err = s.FindRuns(context.Background(), filter)
		if err != nil {
			return err
		}
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"TaskID",
		"Status",
		"ScheduledFor",
		"StartedAt",
		"FinishedAt",
		"RequestedAt",
	)

	for _, r := range runs {
		scheduledFor := r.ScheduledFor.Format(time.RFC3339)
		startedAt := r.StartedAt.Format(time.RFC3339Nano)
		finishedAt := r.FinishedAt.Format(time.RFC3339Nano)
		requestedAt := r.RequestedAt.Format(time.RFC3339Nano)

		w.Write(map[string]interface{}{
			"ID":           r.ID,
			"TaskID":       r.TaskID,
			"Status":       r.Status,
			"ScheduledFor": scheduledFor,
			"StartedAt":    startedAt,
			"FinishedAt":   finishedAt,
			"RequestedAt":  requestedAt,
		})
	}
	w.Flush()

	return nil
}

var runRetryFlags struct {
	taskID, runID string
}

func taskRunRetryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "retry",
		Short: "retry a run",
		RunE:  wrapCheckSetup(runRetryF),
	}

	cmd.Flags().StringVarP(&runRetryFlags.taskID, "task-id", "i", "", "task id (required)")
	cmd.Flags().StringVarP(&runRetryFlags.runID, "run-id", "r", "", "run id (required)")
	cmd.MarkFlagRequired("task-id")
	cmd.MarkFlagRequired("run-id")

	return cmd
}

func runRetryF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:               flags.host,
		Token:              flags.token,
		InsecureSkipVerify: flags.skipVerify,
	}

	var taskID, runID platform.ID
	if err := taskID.DecodeFromString(runRetryFlags.taskID); err != nil {
		return err
	}
	if err := runID.DecodeFromString(runRetryFlags.runID); err != nil {
		return err
	}

	ctx := context.TODO()
	newRun, err := s.RetryRun(ctx, taskID, runID)
	if err != nil {
		return err
	}

	fmt.Printf("Retry for task %s's run %s queued as run %s.\n", taskID, runID, newRun.ID)

	return nil
}
