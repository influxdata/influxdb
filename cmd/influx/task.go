package main

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb/v2"
	"github.com/influxdata/influxdb/v2/http"
	"github.com/influxdata/influxdb/v2/kit/platform"
	"github.com/influxdata/influxdb/v2/task/taskmodel"
	"github.com/influxdata/influxdb/v2/tenant"
	"github.com/spf13/cobra"
)

type taskSVCsFn func() (taskmodel.TaskService, influxdb.OrganizationService, error)

func newTaskSVCs() (taskmodel.TaskService, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	return &http.TaskService{Client: httpClient}, &tenant.OrgClientService{Client: httpClient}, nil
}

func cmdTask(f *globalFlags, opt genericCLIOpts) *cobra.Command {
	builder := newCmdTaskBuilder(newTaskSVCs, f, opt)
	return builder.cmd()
}

type cmdTaskBuilder struct {
	opts        genericCLIOpts
	globalFlags *globalFlags

	svcFn taskSVCsFn

	taskID         string
	runID          string
	taskPrintFlags taskPrintFlags
	// todo: fields of these flags structs could be pulled out for a more streamlined builder struct
	taskCreateFlags      taskCreateFlags
	taskFindFlags        taskFindFlags
	taskRerunFailedFlags taskRerunFailedFlags
	taskUpdateFlags      taskUpdateFlags
	taskRunFindFlags     taskRunFindFlags
	org                  organization
}

func newCmdTaskBuilder(svcsFn taskSVCsFn, f *globalFlags, opts genericCLIOpts) *cmdTaskBuilder {
	return &cmdTaskBuilder{
		globalFlags: f,
		opts:        opts,
		svcFn:       svcsFn,
	}
}

func (b *cmdTaskBuilder) cmd() *cobra.Command {
	cmd := b.newCmd("task", nil)
	cmd.Short = "Task management commands"
	cmd.TraverseChildren = true
	cmd.Run = seeHelp
	cmd.AddCommand(
		b.taskLogCmd(),
		b.taskRunCmd(),
		b.taskCreateCmd(),
		b.taskDeleteCmd(),
		b.taskFindCmd(),
		b.taskUpdateCmd(),
		b.taskRetryFailedCmd(),
	)

	return cmd
}

func (b *cmdTaskBuilder) newCmd(use string, runE func(*cobra.Command, []string) error) *cobra.Command {
	cmd := b.opts.newCmd(use, runE, true)
	b.globalFlags.registerFlags(b.opts.viper, cmd)
	return cmd
}

type taskPrintFlags struct {
	json        bool
	hideHeaders bool
}

type taskCreateFlags struct {
	file string
}

func (b *cmdTaskBuilder) taskCreateCmd() *cobra.Command {
	cmd := b.newCmd("create [script literal or -f /path/to/script.flux]", b.taskCreateF)
	cmd.Args = cobra.MaximumNArgs(1)
	cmd.Short = "Create task"
	cmd.Long = `Create a task with a Flux script provided via the first argument or a file or stdin`

	cmd.Flags().StringVarP(&b.taskCreateFlags.file, "file", "f", "", "Path to Flux script file")
	b.org.register(b.opts.viper, cmd, false)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)

	return cmd
}

func (b *cmdTaskBuilder) taskCreateF(_ *cobra.Command, args []string) error {
	if err := b.org.validOrgFlags(&flags); err != nil {
		return err
	}

	tskSvc, orgSvc, err := b.svcFn()
	if err != nil {
		return err
	}

	flux, err := readFluxQuery(args, b.taskCreateFlags.file)
	if err != nil {
		return fmt.Errorf("error parsing flux script: %s", err)
	}

	tc := taskmodel.TaskCreate{
		Flux:         flux,
		Organization: b.org.name,
	}
	if b.org.id != "" || b.org.name != "" {
		oid, err := b.org.getID(orgSvc)
		if err != nil {
			return fmt.Errorf("error parsing organization ID: %s", err)
		}
		tc.OrganizationID = oid
	}

	tsk, err := tskSvc.CreateTask(context.Background(), tc)
	if err != nil {
		return err
	}

	return b.printTasks(taskPrintOpts{task: tsk})
}

type taskFindFlags struct {
	user    string
	limit   int
	headers bool
}

func (b *cmdTaskBuilder) taskFindCmd() *cobra.Command {
	cmd := b.opts.newCmd("list", b.taskFindF, true)
	cmd.Short = "List tasks"
	cmd.Aliases = []string{"find", "ls"}

	b.org.register(b.opts.viper, cmd, false)
	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "id", "i", "", "task ID")
	cmd.Flags().StringVarP(&b.taskFindFlags.user, "user-id", "n", "", "task owner ID")
	cmd.Flags().IntVarP(&b.taskFindFlags.limit, "limit", "", taskmodel.TaskDefaultPageSize, "the number of tasks to find")
	cmd.Flags().BoolVar(&b.taskFindFlags.headers, "headers", true, "To print the table headers; defaults true")

	return cmd
}

func (b *cmdTaskBuilder) taskFindF(cmd *cobra.Command, args []string) error {

	if err := b.org.validOrgFlags(&flags); err != nil {
		return err
	}

	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := taskmodel.TaskFilter{}
	if b.taskFindFlags.user != "" {
		id, err := platform.IDFromString(b.taskFindFlags.user)
		if err != nil {
			return err
		}
		filter.User = id
	}

	if b.org.name != "" {
		filter.Organization = b.org.name
	}
	if b.org.id != "" {
		id, err := platform.IDFromString(b.org.id)
		if err != nil {
			return err
		}
		filter.OrganizationID = id
	}

	if b.taskFindFlags.limit < 1 || b.taskFindFlags.limit > taskmodel.TaskMaxPageSize {
		return fmt.Errorf("limit must be between 1 and %d", taskmodel.TaskMaxPageSize)
	}
	filter.Limit = b.taskFindFlags.limit

	var tasks []*taskmodel.Task

	if b.taskID != "" {
		id, err := platform.IDFromString(b.taskID)
		if err != nil {
			return err
		}

		task, err := tskSvc.FindTaskByID(context.Background(), *id)
		if err != nil {
			return err
		}

		tasks = append(tasks, task)
	} else {
		tasks, _, err = tskSvc.FindTasks(context.Background(), filter)
		if err != nil {
			return err
		}
	}

	return b.printTasks(taskPrintOpts{tasks: tasks})
}

type taskRerunFailedFlags struct {
	before    string
	after     string
	dryRun    bool
	taskLimit int
	runLimit  int
}

func (b *cmdTaskBuilder) taskRetryFailedCmd() *cobra.Command {
	cmd := b.opts.newCmd("retry-failed", b.taskRetryFailedF, true)
	cmd.Short = "Retry failed runs"
	cmd.Aliases = []string{"rtf"}

	b.org.register(b.opts.viper, cmd, false)
	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "id", "i", "", "task ID")
	cmd.Flags().StringVar(&b.taskRerunFailedFlags.before, "before", "", "runs before this time")
	cmd.Flags().StringVar(&b.taskRerunFailedFlags.after, "after", "", "runs after this time")
	cmd.Flags().BoolVar(&b.taskRerunFailedFlags.dryRun, "dry-run", false,
		"print info about runs that would be retried")
	cmd.Flags().IntVar(&b.taskRerunFailedFlags.taskLimit, "task-limit", 100, "max number of tasks to retry failed runs for")
	cmd.Flags().IntVar(&b.taskRerunFailedFlags.runLimit, "run-limit", 100, "max number of failed runs to retry per task")

	return cmd
}

func (b *cmdTaskBuilder) taskRetryFailedF(*cobra.Command, []string) error {
	if err := b.org.validOrgFlags(&flags); err != nil {
		return err
	}

	if b.taskRerunFailedFlags.taskLimit < 1 || b.taskRerunFailedFlags.taskLimit > 500 {
		return fmt.Errorf("task-limit must be between 1 and 500 (inclusive)")
	}
	if b.taskRerunFailedFlags.runLimit < 1 || b.taskRerunFailedFlags.runLimit > 500 {
		return fmt.Errorf("run-limit must be between 1 and 500 (inclusive)")
	}

	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var failedRuns []*taskmodel.Run
	if b.taskID == "" {
		failedRuns, err = b.getFailedRunsForOrg(b.taskRerunFailedFlags.taskLimit, b.taskRerunFailedFlags.runLimit)
	} else {
		failedRuns, err = b.getFailedRunsForTaskID(b.taskRerunFailedFlags.runLimit)
	}
	if err != nil {
		return err
	}

	for _, run := range failedRuns {
		if b.taskRerunFailedFlags.dryRun {
			fmt.Printf("Would retry for %s run for Task %s.\n", run.ID, run.TaskID)
		} else {
			newRun, err := tskSvc.RetryRun(context.Background(), run.TaskID, run.ID)
			if err != nil {
				return err
			}
			fmt.Printf("Retry for task %s's run %s queued as run %s.\n", run.TaskID, run.ID, newRun.ID)
		}
	}
	if b.taskRerunFailedFlags.dryRun {
		uniqueIDs := make(map[platform.ID]struct{})
		for _, r := range failedRuns {
			uniqueIDs[r.TaskID] = struct{}{}
		}
		fmt.Printf("Dry run complete. Found %d tasks with a total of %d runs to be retried\n"+
			"Rerun without '--dry-run' to execute", len(uniqueIDs), len(failedRuns))
	}

	return nil
}

func (b *cmdTaskBuilder) getFailedRunsForTaskID(limit int) ([]*taskmodel.Run, error) {
	// use RunFilter to search for failed runs
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return nil, err
	}
	runFilter := taskmodel.RunFilter{Limit: limit}
	id, err := platform.IDFromString(b.taskID)
	if err != nil {
		return nil, err
	}
	runFilter.Task = *id
	runFilter.BeforeTime = b.taskRerunFailedFlags.before
	runFilter.AfterTime = b.taskRerunFailedFlags.after
	allRuns, _, err := tskSvc.FindRuns(context.Background(), runFilter)
	if err != nil {
		return nil, err
	}
	var allFailedRuns []*taskmodel.Run
	for _, run := range allRuns {
		if run.Status == "failed" {
			allFailedRuns = append(allFailedRuns, run)
		}
	}
	return allFailedRuns, nil

}

func (b *cmdTaskBuilder) getFailedRunsForOrg(taskLimit int, runLimit int) ([]*taskmodel.Run, error) {
	// use TaskFilter to get all Tasks in org then search for failed runs in each task
	taskFilter := taskmodel.TaskFilter{Limit: taskLimit}
	runFilter := taskmodel.RunFilter{Limit: runLimit}
	runFilter.BeforeTime = b.taskRerunFailedFlags.before
	runFilter.AfterTime = b.taskRerunFailedFlags.after
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return nil, err
	}

	if b.org.name != "" {
		taskFilter.Organization = b.org.name
	}
	if b.org.id != "" {
		orgID, err := platform.IDFromString(b.org.id)
		if err != nil {
			return nil, err
		}
		taskFilter.OrganizationID = orgID
	}

	allTasks, _, err := tskSvc.FindTasks(context.Background(), taskFilter)
	if err != nil {
		return nil, err
	}

	var allFailedRuns []*taskmodel.Run
	for _, t := range allTasks {
		runFilter.Task = t.ID
		runsPerTask, _, err := tskSvc.FindRuns(context.Background(), runFilter)
		var failedRunsPerTask []*taskmodel.Run
		for _, r := range runsPerTask {
			if r.Status == "failed" {
				failedRunsPerTask = append(failedRunsPerTask, r)
			}
		}
		if err != nil {
			return nil, err
		}
		allFailedRuns = append(allFailedRuns, failedRunsPerTask...)
	}
	return allFailedRuns, nil
}

type taskUpdateFlags struct {
	status string
	file   string
}

func (b *cmdTaskBuilder) taskUpdateCmd() *cobra.Command {
	cmd := b.opts.newCmd("update", b.taskUpdateF, true)
	cmd.Short = "Update task"
	cmd.Long = `Update task status or script. Provide a Flux script via the first argument or a file. Use '-' argument to read from stdin.`

	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "id", "i", "", "task ID (required)")
	cmd.Flags().StringVarP(&b.taskUpdateFlags.status, "status", "", "", "update task status")
	cmd.Flags().StringVarP(&b.taskUpdateFlags.file, "file", "f", "", "Path to Flux script file")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdTaskBuilder) taskUpdateF(cmd *cobra.Command, args []string) error {
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var id platform.ID
	if err := id.DecodeFromString(b.taskID); err != nil {
		return err
	}

	var update taskmodel.TaskUpdate
	if b.taskUpdateFlags.status != "" {
		update.Status = &b.taskUpdateFlags.status
	}

	// update flux script only if first arg or file is supplied
	if (len(args) > 0 && len(args[0]) > 0) || len(b.taskUpdateFlags.file) > 0 {
		flux, err := readFluxQuery(args, b.taskUpdateFlags.file)
		if err != nil {
			return fmt.Errorf("error parsing flux script: %s", err)
		}
		update.Flux = &flux
	}

	tsk, err := tskSvc.UpdateTask(context.Background(), id, update)
	if err != nil {
		return err
	}

	return b.printTasks(taskPrintOpts{task: tsk})
}

func (b *cmdTaskBuilder) taskDeleteCmd() *cobra.Command {
	cmd := b.opts.newCmd("delete", b.taskDeleteF, true)
	cmd.Short = "Delete task"

	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "id", "i", "", "task id (required)")
	cmd.MarkFlagRequired("id")

	return cmd
}

func (b *cmdTaskBuilder) taskDeleteF(cmd *cobra.Command, args []string) error {
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var id platform.ID
	err = id.DecodeFromString(b.taskID)
	if err != nil {
		return err
	}

	ctx := context.TODO()
	tsk, err := tskSvc.FindTaskByID(ctx, id)
	if err != nil {
		return err
	}

	if err = tskSvc.DeleteTask(ctx, id); err != nil {
		return err
	}

	return b.printTasks(taskPrintOpts{task: tsk})

}

type taskPrintOpts struct {
	task  *taskmodel.Task
	tasks []*taskmodel.Task
}

func (b *cmdTaskBuilder) printTasks(printOpts taskPrintOpts) error {
	if b.taskPrintFlags.json {
		var v interface{} = printOpts.tasks
		if printOpts.task != nil {
			v = printOpts.task
		}
		return b.opts.writeJSON(v)
	}
	tabW := b.opts.newTabWriter()
	defer tabW.Flush()

	tabW.HideHeaders(b.taskPrintFlags.hideHeaders)

	tabW.WriteHeaders(
		"ID",
		"Name",
		"Organization ID",
		"Organization",
		"Status",
		"Every",
		"Cron",
	)

	if printOpts.task != nil {
		printOpts.tasks = append(printOpts.tasks, printOpts.task)
	}

	for _, t := range printOpts.tasks {
		tabW.Write(map[string]interface{}{
			"ID":              t.ID.String(),
			"Name":            t.Name,
			"Organization ID": t.OrganizationID.String(),
			"Organization":    t.Organization,
			"Status":          t.Status,
			"Every":           t.Every,
			"Cron":            t.Cron,
		})
	}

	return nil
}

func (b *cmdTaskBuilder) taskLogCmd() *cobra.Command {
	cmd := b.opts.newCmd("log", nil, false)
	cmd.Run = seeHelp
	cmd.Short = "Log related commands"
	cmd.AddCommand(
		b.taskLogFindCmd(),
	)

	return cmd
}

func (b *cmdTaskBuilder) taskLogFindCmd() *cobra.Command {
	cmd := b.opts.newCmd("list", b.taskLogFindF, true)
	cmd.Short = "List logs for task"
	cmd.Aliases = []string{"find", "ls"}

	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "task-id", "", "", "task id (required)")
	cmd.Flags().StringVarP(&b.runID, "run-id", "", "", "run id")
	cmd.MarkFlagRequired("task-id")

	return cmd
}

func (b *cmdTaskBuilder) taskLogFindF(cmd *cobra.Command, args []string) error {
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var filter taskmodel.LogFilter
	id, err := platform.IDFromString(b.taskID)
	if err != nil {
		return err
	}
	filter.Task = *id

	if b.runID != "" {
		id, err := platform.IDFromString(b.runID)
		if err != nil {
			return err
		}
		filter.Run = id
	}

	ctx := context.TODO()
	logs, _, err := tskSvc.FindLogs(ctx, filter)
	if err != nil {
		return err
	}

	if b.taskPrintFlags.json {
		return b.opts.writeJSON(logs)
	}

	tabW := b.opts.newTabWriter()
	defer tabW.Flush()

	tabW.HideHeaders(b.taskPrintFlags.hideHeaders)

	tabW.WriteHeaders("RunID", "Time", "Message")
	for _, log := range logs {
		tabW.Write(map[string]interface{}{
			"RunID":   log.RunID,
			"Time":    log.Time,
			"Message": log.Message,
		})
	}

	return nil
}

func (b *cmdTaskBuilder) taskRunCmd() *cobra.Command {
	cmd := b.opts.newCmd("run", nil, false)
	cmd.Run = seeHelp
	cmd.Short = "List runs for a task"
	cmd.AddCommand(
		b.taskRunFindCmd(),
		b.taskRunRetryCmd(),
	)

	return cmd
}

type taskRunFindFlags struct {
	afterTime  string
	beforeTime string
	limit      int
}

func (b *cmdTaskBuilder) taskRunFindCmd() *cobra.Command {
	cmd := b.opts.newCmd("list", b.taskRunFindF, true)
	cmd.Short = "List runs for a task"
	cmd.Aliases = []string{"find", "ls"}

	b.globalFlags.registerFlags(b.opts.viper, cmd)
	registerPrintOptions(b.opts.viper, cmd, &b.taskPrintFlags.hideHeaders, &b.taskPrintFlags.json)
	cmd.Flags().StringVarP(&b.taskID, "task-id", "", "", "task id (required)")
	cmd.Flags().StringVarP(&b.runID, "run-id", "", "", "run id")
	cmd.Flags().StringVarP(&b.taskRunFindFlags.afterTime, "after", "", "", "after time for filtering")
	cmd.Flags().StringVarP(&b.taskRunFindFlags.beforeTime, "before", "", "", "before time for filtering")
	cmd.Flags().IntVarP(&b.taskRunFindFlags.limit, "limit", "", 100, "limit the results; default is 100")

	cmd.MarkFlagRequired("task-id")

	return cmd
}

func (b *cmdTaskBuilder) taskRunFindF(cmd *cobra.Command, args []string) error {
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	filter := taskmodel.RunFilter{
		Limit:      b.taskRunFindFlags.limit,
		AfterTime:  b.taskRunFindFlags.afterTime,
		BeforeTime: b.taskRunFindFlags.beforeTime,
	}
	taskID, err := platform.IDFromString(b.taskID)
	if err != nil {
		return err
	}
	filter.Task = *taskID

	var runs []*taskmodel.Run
	if b.runID != "" {
		id, err := platform.IDFromString(b.runID)
		if err != nil {
			return err
		}
		run, err := tskSvc.FindRunByID(context.Background(), filter.Task, *id)
		if err != nil {
			return err
		}
		runs = append(runs, run)
	} else {
		runs, _, err = tskSvc.FindRuns(context.Background(), filter)
		if err != nil {
			return err
		}
	}

	if b.taskPrintFlags.json {
		if runs == nil {
			// guarantee we never return a null value from CLI
			runs = make([]*taskmodel.Run, 0)
		}
		return b.opts.writeJSON(runs)
	}

	tabW := b.opts.newTabWriter()
	defer tabW.Flush()

	tabW.HideHeaders(b.taskPrintFlags.hideHeaders)

	tabW.WriteHeaders(
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

		tabW.Write(map[string]interface{}{
			"ID":           r.ID,
			"TaskID":       r.TaskID,
			"Status":       r.Status,
			"ScheduledFor": scheduledFor,
			"StartedAt":    startedAt,
			"FinishedAt":   finishedAt,
			"RequestedAt":  requestedAt,
		})
	}

	return nil
}

var runRetryFlags struct {
	taskID, runID string
}

func (b *cmdTaskBuilder) taskRunRetryCmd() *cobra.Command {
	cmd := b.opts.newCmd("retry", b.runRetryF, true)
	cmd.Short = "retry a run"

	b.globalFlags.registerFlags(b.opts.viper, cmd)
	cmd.Flags().StringVarP(&runRetryFlags.taskID, "task-id", "i", "", "task id (required)")
	cmd.Flags().StringVarP(&runRetryFlags.runID, "run-id", "r", "", "run id (required)")
	cmd.MarkFlagRequired("task-id")
	cmd.MarkFlagRequired("run-id")

	return cmd
}

func (b *cmdTaskBuilder) runRetryF(*cobra.Command, []string) error {
	tskSvc, _, err := b.svcFn()
	if err != nil {
		return err
	}

	var taskID, runID platform.ID
	if err := taskID.DecodeFromString(runRetryFlags.taskID); err != nil {
		return err
	}
	if err := runID.DecodeFromString(runRetryFlags.runID); err != nil {
		return err
	}

	ctx := context.TODO()
	newRun, err := tskSvc.RetryRun(ctx, taskID, runID)
	if err != nil {
		return err
	}

	fmt.Printf("Retry for task %s's run %s queued as run %s.\n", taskID, runID, newRun.ID)

	return nil
}
