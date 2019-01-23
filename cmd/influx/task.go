package main

import (
	"context"
	"fmt"
	"os"

	"github.com/influxdata/flux/repl"
	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influx/internal"
	"github.com/influxdata/influxdb/http"
	"github.com/spf13/cobra"
)

// task Command
var taskCmd = &cobra.Command{
	Use:   "task",
	Short: "Task management commands",
	RunE:  wrapCheckSetup(taskF),
}

func taskF(cmd *cobra.Command, args []string) error {
	if flags.local {
		return fmt.Errorf("local flag not supported for task command")
	}

	cmd.Usage()
	return nil
}

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Log related commands",
	Run:   logF,
}

func logF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run related commands",
	Run:   runF,
}

func runF(cmd *cobra.Command, args []string) {
	cmd.Usage()
}

func init() {
	taskCmd.AddCommand(runCmd)
	taskCmd.AddCommand(logCmd)
}

// TaskCreateFlags define the Create Command
type TaskCreateFlags struct {
	org   string
	orgID string
}

var taskCreateFlags TaskCreateFlags

func init() {
	taskCreateCmd := &cobra.Command{
		Use:   "create [query literal or @/path/to/query.flux]",
		Short: "Create task",
		Args:  cobra.ExactArgs(1),
		RunE:  wrapCheckSetup(taskCreateF),
	}

	taskCreateCmd.Flags().StringVarP(&taskCreateFlags.org, "org", "", "", "organization name")
	taskCreateCmd.Flags().StringVarP(&taskCreateFlags.orgID, "org-id", "", "", "id of the organization that owns the task")
	taskCreateCmd.MarkFlagRequired("flux")

	taskCmd.AddCommand(taskCreateCmd)
}

func taskCreateF(cmd *cobra.Command, args []string) error {
	if taskCreateFlags.org != "" && taskCreateFlags.orgID != "" {
		return fmt.Errorf("must specify exactly one of org or org-id")
	}

	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	flux, err := repl.LoadQuery(args[0])
	if err != nil {
		return fmt.Errorf("error parsing flux script: %s", err)
	}

	t := &platform.Task{
		Flux: flux,
	}

	if taskCreateFlags.org != "" && taskCreateFlags.orgID == "" {
		ow := &http.OrganizationService{
			Addr:  flags.host,
			Token: flags.token,
		}

		filter := platform.OrganizationFilter{
			Name: &taskCreateFlags.org,
		}

		orgs, _, err := ow.FindOrganizations(context.Background(), filter)
		if err != nil {
			return err
		}

		if len(orgs) != 1 {
			fmt.Println("Unable to find a single org matching that ID.")
			w := internal.NewTabWriter(os.Stdout)
			w.WriteHeaders(
				"ID",
				"Name",
			)
			for _, o := range orgs {
				w.Write(map[string]interface{}{
					"ID":   o.ID.String(),
					"Name": o.Name,
				})
			}
			w.Flush()
		}

		t.OrganizationID = orgs[0].ID
		t.Organization = orgs[0].Name
	}

	if taskCreateFlags.orgID != "" {
		id, err := platform.IDFromString(taskCreateFlags.orgID)
		if err != nil {
			return fmt.Errorf("error parsing organization id: %v", err)
		}
		t.OrganizationID = *id
	}

	if err := s.CreateTask(context.Background(), t); err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
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

// taskFindFlags define the Find Command
type TaskFindFlags struct {
	user  string
	id    string
	orgID string
	limit int
}

var taskFindFlags TaskFindFlags

func init() {
	taskFindCmd := &cobra.Command{
		Use:   "find",
		Short: "Find tasks",
		RunE:  wrapCheckSetup(taskFindF),
	}

	taskFindCmd.Flags().StringVarP(&taskFindFlags.id, "id", "i", "", "task ID")
	taskFindCmd.Flags().StringVarP(&taskFindFlags.user, "user-id", "n", "", "task owner ID")
	taskFindCmd.Flags().StringVarP(&taskFindFlags.orgID, "org-id", "", "", "task organization ID")
	taskFindCmd.Flags().IntVarP(&taskFindFlags.limit, "limit", "", platform.TaskDefaultPageSize, "the number of tasks to find")

	taskCmd.AddCommand(taskFindCmd)
}

func taskFindF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	filter := platform.TaskFilter{}
	if taskFindFlags.user != "" {
		id, err := platform.IDFromString(taskFindFlags.user)
		if err != nil {
			return err
		}
		filter.User = id
	}

	if taskFindFlags.orgID != "" {
		id, err := platform.IDFromString(taskFindFlags.orgID)
		if err != nil {
			return err
		}
		filter.Organization = id
	}

	if taskFindFlags.limit < 1 || taskFindFlags.limit > platform.TaskMaxPageSize {
		return fmt.Errorf("limit must be between 1 and %d", platform.TaskMaxPageSize)
	}
	filter.Limit = taskFindFlags.limit

	var tasks []*platform.Task
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

		tasks = append(tasks, task)
	} else {
		tasks, _, err = s.FindTasks(context.Background(), filter)
		if err != nil {
			return err
		}
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"ID",
		"Name",
		"OrganizationID",
		"Organization",
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

// taskUpdateFlags define the Update Command
type TaskUpdateFlags struct {
	id     string
	status string
}

var taskUpdateFlags TaskUpdateFlags

func init() {
	taskUpdateCmd := &cobra.Command{
		Use:   "update",
		Short: "Update task",
		RunE:  wrapCheckSetup(taskUpdateF),
	}

	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.id, "id", "i", "", "task ID (required)")
	taskUpdateCmd.Flags().StringVarP(&taskUpdateFlags.status, "status", "", "", "update task status")
	taskUpdateCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskUpdateCmd)
}

func taskUpdateF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
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

// taskDeleteFlags define the Delete command
type TaskDeleteFlags struct {
	id string
}

var taskDeleteFlags TaskDeleteFlags

func init() {
	taskDeleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete task",
		RunE:  wrapCheckSetup(taskDeleteF),
	}

	taskDeleteCmd.Flags().StringVarP(&taskDeleteFlags.id, "id", "i", "", "task id (required)")
	taskDeleteCmd.MarkFlagRequired("id")

	taskCmd.AddCommand(taskDeleteCmd)
}

func taskDeleteF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
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

// taskLogFindFlags define the Delete command
type TaskLogFindFlags struct {
	taskID string
	runID  string
	orgID  string
}

var taskLogFindFlags TaskLogFindFlags

func init() {
	taskLogFindCmd := &cobra.Command{
		Use:   "find",
		Short: "find logs for task",
		RunE:  wrapCheckSetup(taskLogFindF),
	}

	taskLogFindCmd.Flags().StringVarP(&taskLogFindFlags.taskID, "task-id", "", "", "task id (required)")
	taskLogFindCmd.Flags().StringVarP(&taskLogFindFlags.runID, "run-id", "", "", "run id")
	taskLogFindCmd.Flags().StringVarP(&taskLogFindFlags.orgID, "org-id", "", "", "organization id")
	taskLogFindCmd.MarkFlagRequired("task-id")

	logCmd.AddCommand(taskLogFindCmd)
}

func taskLogFindF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
	}

	var filter platform.LogFilter
	id, err := platform.IDFromString(taskLogFindFlags.taskID)
	if err != nil {
		return err
	}
	filter.Task = id

	if taskLogFindFlags.runID != "" {
		id, err := platform.IDFromString(taskLogFindFlags.runID)
		if err != nil {
			return err
		}
		filter.Run = id
	}

	if taskLogFindFlags.orgID != "" {
		id, err := platform.IDFromString(taskLogFindFlags.orgID)
		if err != nil {
			return err
		}
		filter.Org = id
	}

	ctx := context.TODO()
	logs, _, err := s.FindLogs(ctx, filter)
	if err != nil {
		return err
	}

	w := internal.NewTabWriter(os.Stdout)
	w.WriteHeaders(
		"Log",
	)
	for _, log := range logs {
		w.Write(map[string]interface{}{
			"Log": *log,
		})
	}
	w.Flush()

	return nil
}

// taskLogFindFlags define the Delete command
type TaskRunFindFlags struct {
	runID      string
	taskID     string
	orgID      string
	afterTime  string
	beforeTime string
	limit      int
}

var taskRunFindFlags TaskRunFindFlags

func init() {
	taskRunFindCmd := &cobra.Command{
		Use:   "find",
		Short: "find runs for a task",
		RunE:  wrapCheckSetup(taskRunFindF),
	}

	taskRunFindCmd.Flags().StringVarP(&taskRunFindFlags.taskID, "task-id", "", "", "task id (required)")
	taskRunFindCmd.Flags().StringVarP(&taskRunFindFlags.runID, "run-id", "", "", "run id")
	taskRunFindCmd.Flags().StringVarP(&taskRunFindFlags.orgID, "org-id", "", "", "organization id")
	taskRunFindCmd.Flags().StringVarP(&taskRunFindFlags.afterTime, "after", "", "", "after time for filtering")
	taskRunFindCmd.Flags().StringVarP(&taskRunFindFlags.beforeTime, "before", "", "", "before time for filtering")
	taskRunFindCmd.Flags().IntVarP(&taskRunFindFlags.limit, "limit", "", 0, "limit the results")

	taskRunFindCmd.MarkFlagRequired("task-id")
	taskRunFindCmd.MarkFlagRequired("org-id")

	runCmd.AddCommand(taskRunFindCmd)
}

func taskRunFindF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
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
	filter.Task = taskID

	orgID, err := platform.IDFromString(taskRunFindFlags.orgID)
	if err != nil {
		return err
	}
	filter.Org = orgID

	var runs []*platform.Run
	if taskRunFindFlags.runID != "" {
		id, err := platform.IDFromString(taskRunFindFlags.runID)
		if err != nil {
			return err
		}
		run, err := s.FindRunByID(context.Background(), *filter.Org, *id)
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
		w.Write(map[string]interface{}{
			"ID":           r.ID,
			"TaskID":       r.TaskID,
			"Status":       r.Status,
			"ScheduledFor": r.ScheduledFor,
			"StartedAt":    r.StartedAt,
			"FinishedAt":   r.FinishedAt,
			"RequestedAt":  r.RequestedAt,
		})
	}
	w.Flush()

	return nil
}

type RunRetryFlags struct {
	taskID, runID string
}

var runRetryFlags RunRetryFlags

func init() {
	cmd := &cobra.Command{
		Use:   "retry",
		Short: "retry a run",
		RunE:  wrapCheckSetup(runRetryF),
	}

	cmd.Flags().StringVarP(&runRetryFlags.taskID, "task-id", "i", "", "task id (required)")
	cmd.Flags().StringVarP(&runRetryFlags.runID, "run-id", "r", "", "run id (required)")
	cmd.MarkFlagRequired("task-id")
	cmd.MarkFlagRequired("run-id")

	runCmd.AddCommand(cmd)
}

func runRetryF(cmd *cobra.Command, args []string) error {
	s := &http.TaskService{
		Addr:  flags.host,
		Token: flags.token,
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
