package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/influxdata/influxdb/v2"
	ihttp "github.com/influxdata/influxdb/v2/http"
	ierror "github.com/influxdata/influxdb/v2/kit/errors"
	"github.com/influxdata/influxdb/v2/pkger"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
)

type pkgSVCsFn func() (pkger.SVC, influxdb.OrganizationService, error)

func cmdPkg(f *globalFlags, opts genericCLIOpts) *cobra.Command {
	return newCmdPkgBuilder(newPkgerSVC, opts).cmd()
}

type cmdPkgBuilder struct {
	genericCLIOpts

	svcFn pkgSVCsFn

	encoding            string
	file                string
	files               []string
	filters             []string
	description         string
	disableColor        bool
	disableTableBorders bool
	hideHeaders         bool
	json                bool
	name                string
	names               []string
	org                 organization
	quiet               bool
	recurse             bool
	stackID             string
	stackIDs            []string
	urls                []string

	applyOpts struct {
		envRefs []string
		force   string
		secrets []string
	}

	exportOpts struct {
		resourceType string
		buckets      string
		checks       string
		dashboards   string
		endpoints    string
		labels       string
		rules        string
		tasks        string
		telegrafs    string
		variables    string
	}
}

func newCmdPkgBuilder(svcFn pkgSVCsFn, opts genericCLIOpts) *cmdPkgBuilder {
	return &cmdPkgBuilder{
		genericCLIOpts: opts,
		svcFn:          svcFn,
	}
}

func (b *cmdPkgBuilder) cmd() *cobra.Command {
	cmd := b.cmdPkgApply()
	cmd.AddCommand(
		b.cmdPkgExport(),
		b.cmdPkgSummary(),
		b.cmdPkgValidate(),
		b.cmdStack(),
	)

	return cmd
}

func (b *cmdPkgBuilder) cmdPkgApply() *cobra.Command {
	cmd := b.newCmd("pkg", b.pkgApplyRunEFn, true)
	cmd.Short = "Apply a pkg to create resources"

	b.org.register(cmd, false)
	b.registerPkgFileFlags(cmd)
	b.registerPkgPrintOpts(cmd)
	cmd.Flags().BoolVarP(&b.quiet, "quiet", "q", false, "Disable output printing")
	cmd.Flags().StringVar(&b.applyOpts.force, "force", "", `TTY input, if package will have destructive changes, proceed if set "true"`)
	cmd.Flags().StringVar(&b.stackID, "stack-id", "", "Stack ID to associate pkg application")

	b.applyOpts.secrets = []string{}
	cmd.Flags().StringSliceVar(&b.applyOpts.secrets, "secret", nil, "Secrets to provide alongside the package; format should --secret=SECRET_KEY=SECRET_VALUE --secret=SECRET_KEY_2=SECRET_VALUE_2")
	cmd.Flags().StringSliceVar(&b.applyOpts.envRefs, "env-ref", nil, "Environment references to provide alongside the package; format should --env-ref=REF_KEY=REF_VALUE --env-ref=REF_KEY_2=REF_VALUE_2")

	return cmd
}

func (b *cmdPkgBuilder) pkgApplyRunEFn(cmd *cobra.Command, args []string) error {
	if err := b.org.validOrgFlags(&flags); err != nil {
		return err
	}
	color.NoColor = b.disableColor

	svc, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	influxOrgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	pkg, isTTY, err := b.readPkg()
	if err != nil {
		return err
	}

	providedEnvRefs := mapKeys(pkg.Summary().MissingEnvs, b.applyOpts.envRefs)
	if !isTTY {
		for _, envRef := range missingValKeys(providedEnvRefs) {
			prompt := "Please provide environment reference value for key " + envRef
			providedEnvRefs[envRef] = b.getInput(prompt, "")
		}
	}

	var stackID influxdb.ID
	if b.stackID != "" {
		if err := stackID.DecodeFromString(b.stackID); err != nil {
			return err
		}
	}

	opts := []pkger.ApplyOptFn{
		pkger.ApplyWithEnvRefs(providedEnvRefs),
		pkger.ApplyWithStackID(stackID),
	}

	drySum, diff, err := svc.DryRun(context.Background(), influxOrgID, 0, pkg, opts...)
	if err != nil {
		return err
	}

	providedSecrets := mapKeys(drySum.MissingSecrets, b.applyOpts.secrets)
	if !isTTY {
		const skipDefault = "$$skip-this-key$$"
		for _, secretKey := range missingValKeys(providedSecrets) {
			prompt := "Please provide secret value for key " + secretKey + " (optional, press enter to skip)"
			secretVal := b.getInput(prompt, skipDefault)
			if secretVal != "" && secretVal != skipDefault {
				providedSecrets[secretKey] = secretVal
			}
		}
	}

	if err := b.printPkgDiff(diff); err != nil {
		return err
	}

	isForced, _ := strconv.ParseBool(b.applyOpts.force)
	if !isTTY && !isForced && b.applyOpts.force != "conflict" {
		confirm := b.getInput("Confirm application of the above resources (y/n)", "n")
		if strings.ToLower(confirm) != "y" {
			fmt.Fprintln(b.w, "aborted application of package")
			return nil
		}
	}

	if b.applyOpts.force != "conflict" && isTTY && diff.HasConflicts() {
		return errors.New("package has conflicts with existing resources and cannot safely apply")
	}

	opts = append(opts, pkger.ApplyWithSecrets(providedSecrets))

	summary, _, err := svc.Apply(context.Background(), influxOrgID, 0, pkg, opts...)
	if err != nil {
		return err
	}

	b.printPkgSummary(summary)

	return nil
}

func (b *cmdPkgBuilder) cmdPkgExport() *cobra.Command {
	cmd := b.newCmd("export", b.pkgExportRunEFn, true)
	cmd.Short = "Export existing resources as a package"
	cmd.AddCommand(b.cmdPkgExportAll())

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().StringVar(&b.exportOpts.resourceType, "resource-type", "", "The resource type provided will be associated with all IDs via stdin.")
	cmd.Flags().StringVar(&b.exportOpts.buckets, "buckets", "", "List of bucket ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.checks, "checks", "", "List of check ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.dashboards, "dashboards", "", "List of dashboard ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.endpoints, "endpoints", "", "List of notification endpoint ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.labels, "labels", "", "List of label ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.rules, "rules", "", "List of notification rule ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.tasks, "tasks", "", "List of task ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.telegrafs, "telegraf-configs", "", "List of telegraf config ids comma separated")
	cmd.Flags().StringVar(&b.exportOpts.variables, "variables", "", "List of variable ids comma separated")

	return cmd
}

func (b *cmdPkgBuilder) pkgExportRunEFn(cmd *cobra.Command, args []string) error {
	pkgSVC, _, err := b.svcFn()
	if err != nil {
		return err
	}

	opts := []pkger.CreatePkgSetFn{}

	resTypes := []struct {
		kind   pkger.Kind
		idStrs []string
	}{
		{kind: pkger.KindBucket, idStrs: strings.Split(b.exportOpts.buckets, ",")},
		{kind: pkger.KindCheck, idStrs: strings.Split(b.exportOpts.checks, ",")},
		{kind: pkger.KindDashboard, idStrs: strings.Split(b.exportOpts.dashboards, ",")},
		{kind: pkger.KindLabel, idStrs: strings.Split(b.exportOpts.labels, ",")},
		{kind: pkger.KindNotificationEndpoint, idStrs: strings.Split(b.exportOpts.endpoints, ",")},
		{kind: pkger.KindNotificationRule, idStrs: strings.Split(b.exportOpts.rules, ",")},
		{kind: pkger.KindTask, idStrs: strings.Split(b.exportOpts.tasks, ",")},
		{kind: pkger.KindTelegraf, idStrs: strings.Split(b.exportOpts.telegrafs, ",")},
		{kind: pkger.KindVariable, idStrs: strings.Split(b.exportOpts.variables, ",")},
	}
	for _, rt := range resTypes {
		newOpt, err := newResourcesToClone(rt.kind, rt.idStrs)
		if err != nil {
			return ierror.Wrap(err, rt.kind.String())
		}
		opts = append(opts, newOpt)
	}

	if b.exportOpts.resourceType == "" {
		return b.writePkg(cmd.OutOrStdout(), pkgSVC, b.file, opts...)
	}

	kind := pkger.Kind(b.exportOpts.resourceType)
	if err := kind.OK(); err != nil {
		return errors.New("resource type must be one of bucket|dashboard|label|variable; got: " + b.exportOpts.resourceType)
	}

	if stdin, err := b.inStdIn(); err == nil {
		stdinInpt, _ := b.readLines(stdin)
		if len(stdinInpt) > 0 {
			args = stdinInpt
		}
	}

	resTypeOpt, err := newResourcesToClone(kind, args)
	if err != nil {
		return err
	}

	return b.writePkg(cmd.OutOrStdout(), pkgSVC, b.file, append(opts, resTypeOpt)...)
}

func (b *cmdPkgBuilder) cmdPkgExportAll() *cobra.Command {
	cmd := b.newCmd("all", b.pkgExportAllRunEFn, true)
	cmd.Short = "Export all existing resources for an organization as a package"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().StringArrayVar(&b.filters, "filter", nil, "Filter exported resources by labelName or resourceKind (format: --filter=labelName=example)")

	b.org.register(cmd, false)

	return cmd
}

func (b *cmdPkgBuilder) pkgExportAllRunEFn(cmd *cobra.Command, args []string) error {
	pkgSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	var (
		labelNames    []string
		resourceKinds []pkger.Kind
	)
	for _, filter := range b.filters {
		pair := strings.SplitN(filter, "=", 2)
		if len(pair) < 2 {
			continue
		}
		switch key, val := pair[0], pair[1]; key {
		case "labelName":
			labelNames = append(labelNames, val)
		case "resourceKind":
			k := pkger.Kind(val)
			if err := k.OK(); err != nil {
				return err
			}
			resourceKinds = append(resourceKinds, k)
		default:
			return fmt.Errorf("invalid filter provided %q; filter must be 1 in [labelName, resourceKind]", filter)
		}
	}

	orgOpt := pkger.CreateWithAllOrgResources(pkger.CreateByOrgIDOpt{
		OrgID:         orgID,
		LabelNames:    labelNames,
		ResourceKinds: resourceKinds,
	})
	return b.writePkg(cmd.OutOrStdout(), pkgSVC, b.file, orgOpt)
}

func (b *cmdPkgBuilder) cmdPkgSummary() *cobra.Command {
	runE := func(cmd *cobra.Command, args []string) error {
		pkg, _, err := b.readPkg()
		if err != nil {
			return err
		}

		return b.printPkgSummary(pkg.Summary())
	}

	cmd := b.newCmd("summary", runE, false)
	cmd.Short = "Summarize the provided package"

	b.registerPkgFileFlags(cmd)
	b.registerPkgPrintOpts(cmd)

	return cmd
}

func (b *cmdPkgBuilder) cmdPkgValidate() *cobra.Command {
	runE := func(cmd *cobra.Command, args []string) error {
		pkg, _, err := b.readPkg()
		if err != nil {
			return err
		}
		return pkg.Validate()
	}

	cmd := b.newCmd("validate", runE, false)
	cmd.Short = "Validate the provided package"

	b.registerPkgFileFlags(cmd)

	return cmd
}

func (b *cmdPkgBuilder) cmdStack() *cobra.Command {
	cmd := b.newCmd("stack", nil, false)
	cmd.Short = "Stack management commands"
	cmd.AddCommand(
		b.cmdStackInit(),
		b.cmdStackList(),
		b.cmdStackRemove(),
	)
	return cmd
}

func (b *cmdPkgBuilder) cmdStackInit() *cobra.Command {
	cmd := b.newCmd("init", b.stackInitRunEFn, true)
	cmd.Short = "Initialize a stack"

	cmd.Flags().StringVarP(&b.name, "stack-name", "n", "", "Name given to created stack")
	cmd.Flags().StringVarP(&b.description, "stack-description", "d", "", "Description given to created stack")
	cmd.Flags().StringArrayVarP(&b.urls, "package-url", "u", nil, "Package urls to associate with new stack")
	registerPrintOptions(cmd, &b.hideHeaders, &b.json)

	b.org.register(cmd, false)

	return cmd
}

func (b *cmdPkgBuilder) stackInitRunEFn(cmd *cobra.Command, args []string) error {
	pkgSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	const fakeUserID = 0 // is 0 because user is pulled from token...
	stack, err := pkgSVC.InitStack(context.Background(), fakeUserID, pkger.Stack{
		OrgID:       orgID,
		Name:        b.name,
		Description: b.description,
		URLs:        b.urls,
	})
	if err != nil {
		return err
	}

	if b.json {
		return b.writeJSON(stack)
	}

	tabW := b.newTabWriter()
	defer tabW.Flush()

	tabW.HideHeaders(b.hideHeaders)

	tabW.WriteHeaders("ID", "OrgID", "Name", "Description", "URLs", "Created At")
	tabW.Write(map[string]interface{}{
		"ID":          stack.ID,
		"OrgID":       stack.OrgID,
		"Name":        stack.Name,
		"Description": stack.Description,
		"URLs":        stack.URLs,
		"Created At":  stack.CreatedAt,
	})

	return nil
}

func (b *cmdPkgBuilder) cmdStackList() *cobra.Command {
	cmd := b.newCmd("list [flags]", b.stackListRunEFn, true)
	cmd.Short = "List stack(s) and associated resources"
	cmd.Aliases = []string{"ls"}

	cmd.Flags().StringArrayVar(&b.stackIDs, "stack-id", nil, "Stack IDs to filter by")
	cmd.Flags().StringArrayVar(&b.names, "stack-name", nil, "Stack names to filter by")
	registerPrintOptions(cmd, &b.hideHeaders, &b.json)

	b.org.register(cmd, false)

	return cmd
}

func (b *cmdPkgBuilder) stackListRunEFn(cmd *cobra.Command, args []string) error {
	pkgSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	var stackIDs []influxdb.ID
	for _, rawID := range b.stackIDs {
		id, err := influxdb.IDFromString(rawID)
		if err != nil {
			return err
		}
		stackIDs = append(stackIDs, *id)
	}

	stacks, err := pkgSVC.ListStacks(context.Background(), orgID, pkger.ListFilter{
		StackIDs: stackIDs,
		Names:    b.names,
	})
	if err != nil {
		return err
	}

	if b.json {
		return b.writeJSON(stacks)
	}

	tabW := b.newTabWriter()
	defer tabW.Flush()

	tabW.HideHeaders(b.hideHeaders)
	tabW.WriteHeaders("ID", "OrgID", "Name", "Description", "Num Resources", "URLs", "Created At")

	for _, stack := range stacks {
		tabW.Write(map[string]interface{}{
			"ID":            stack.ID,
			"OrgID":         stack.OrgID,
			"Name":          stack.Name,
			"Description":   stack.Description,
			"Num Resources": len(stack.Resources),
			"URLs":          stack.URLs,
			"Created At":    stack.CreatedAt,
		})
	}

	return nil
}

func (b *cmdPkgBuilder) cmdStackRemove() *cobra.Command {
	cmd := b.newCmd("remove [--stack-id=ID1 --stack-id=ID2]", b.stackRemoveRunEFn, true)
	cmd.Short = "Remove a stack(s) and all associated resources"
	cmd.Aliases = []string{"rm", "uninstall"}

	cmd.Flags().StringArrayVar(&b.stackIDs, "stack-id", nil, "Stack IDs to be removed")
	cmd.MarkFlagRequired("stack-id")
	registerPrintOptions(cmd, &b.hideHeaders, &b.json)

	b.org.register(cmd, false)

	return cmd
}

func (b *cmdPkgBuilder) stackRemoveRunEFn(cmd *cobra.Command, args []string) error {
	pkgSVC, orgSVC, err := b.svcFn()
	if err != nil {
		return err
	}

	orgID, err := b.org.getID(orgSVC)
	if err != nil {
		return err
	}

	var stackIDs []influxdb.ID
	for _, rawID := range b.stackIDs {
		id, err := influxdb.IDFromString(rawID)
		if err != nil {
			return err
		}
		stackIDs = append(stackIDs, *id)
	}

	stacks, err := pkgSVC.ListStacks(context.Background(), orgID, pkger.ListFilter{
		StackIDs: stackIDs,
	})
	if err != nil {
		return err
	}

	printStack := func(stack pkger.Stack) error {
		if b.json {
			return b.writeJSON(stack)
		}

		tabW := b.newTabWriter()
		defer func() {
			tabW.Flush()
			// add a breather line between confirm and printout
			fmt.Fprintln(b.w)
		}()

		tabW.HideHeaders(b.hideHeaders)

		tabW.WriteHeaders("ID", "OrgID", "Name", "Description", "Num Resources", "URLs", "Created At")
		tabW.Write(map[string]interface{}{
			"ID":            stack.ID,
			"OrgID":         stack.OrgID,
			"Name":          stack.Name,
			"Description":   stack.Description,
			"Num Resources": len(stack.Resources),
			"URLs":          stack.URLs,
			"Created At":    stack.CreatedAt,
		})

		return nil
	}

	for _, stack := range stacks {
		if err := printStack(stack); err != nil {
			return err
		}

		msg := fmt.Sprintf("Confirm removal of the stack[%s] and all associated resources (y/n)", stack.ID)
		confirm := b.getInput(msg, "n")
		if confirm != "y" {
			continue
		}

		err := pkgSVC.DeleteStack(context.Background(), struct{ OrgID, UserID, StackID influxdb.ID }{
			OrgID:   orgID,
			UserID:  0,
			StackID: stack.ID,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *cmdPkgBuilder) registerPkgPrintOpts(cmd *cobra.Command) {
	cmd.Flags().BoolVarP(&b.disableColor, "disable-color", "c", false, "Disable color in output")
	cmd.Flags().BoolVar(&b.disableTableBorders, "disable-table-borders", false, "Disable table borders")
	registerPrintOptions(cmd, nil, &b.json)
}

func (b *cmdPkgBuilder) registerPkgFileFlags(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&b.files, "file", "f", nil, "Path to package file")
	cmd.MarkFlagFilename("file", "yaml", "yml", "json", "jsonnet")
	cmd.Flags().BoolVarP(&b.recurse, "recurse", "R", false, "Process the directory used in -f, --file recursively. Useful when you want to manage related manifests organized within the same directory.")

	cmd.Flags().StringSliceVarP(&b.urls, "url", "u", nil, "URL to a package file")

	cmd.Flags().StringVarP(&b.encoding, "encoding", "e", "", "Encoding for the input stream. If a file is provided will gather encoding type from file extension. If extension provided will override.")
	cmd.MarkFlagFilename("encoding", "yaml", "yml", "json", "jsonnet")
}

func (b *cmdPkgBuilder) writePkg(w io.Writer, pkgSVC pkger.SVC, outPath string, opts ...pkger.CreatePkgSetFn) error {
	pkg, err := pkgSVC.CreatePkg(context.Background(), opts...)
	if err != nil {
		return err
	}

	buf, err := createPkgBuf(pkg, outPath)
	if err != nil {
		return err
	}

	if outPath == "" {
		_, err := io.Copy(w, buf)
		return err
	}

	return ioutil.WriteFile(outPath, buf.Bytes(), os.ModePerm)
}

func (b *cmdPkgBuilder) readRawPkgsFromFiles(filePaths []string, recurse bool) ([]*pkger.Pkg, error) {
	mFiles := make(map[string]struct{})
	for _, f := range filePaths {
		files, err := readFilesFromPath(f, recurse)
		if err != nil {
			return nil, err
		}
		for _, ff := range files {
			mFiles[ff] = struct{}{}
		}
	}

	var rawPkgs []*pkger.Pkg
	for f := range mFiles {
		pkg, err := pkger.Parse(b.convertFileEncoding(f), pkger.FromFile(f), pkger.ValidSkipParseError())
		if err != nil {
			return nil, err
		}
		rawPkgs = append(rawPkgs, pkg)
	}

	return rawPkgs, nil
}

func (b *cmdPkgBuilder) readRawPkgsFromURLs(urls []string) ([]*pkger.Pkg, error) {
	mURLs := make(map[string]struct{})
	for _, f := range urls {
		mURLs[f] = struct{}{}
	}

	var rawPkgs []*pkger.Pkg
	for u := range mURLs {
		pkg, err := pkger.Parse(b.convertURLEncoding(u), pkger.FromHTTPRequest(u), pkger.ValidSkipParseError())
		if err != nil {
			return nil, err
		}
		rawPkgs = append(rawPkgs, pkg)
	}
	return rawPkgs, nil
}

func (b *cmdPkgBuilder) readPkg() (*pkger.Pkg, bool, error) {
	pkgs, err := b.readRawPkgsFromFiles(b.files, b.recurse)
	if err != nil {
		return nil, false, err
	}

	urlPkgs, err := b.readRawPkgsFromURLs(b.urls)
	if err != nil {
		return nil, false, err
	}
	pkgs = append(pkgs, urlPkgs...)

	// the pkger.ValidSkipParseError option allows our server to be the one to validate the
	// the pkg is accurate. If a user has an older version of the CLI and cloud gets updated
	// with new validation rules,they'll get immediate access to that change without having to
	// rol their CLI build.

	if _, err := b.inStdIn(); err != nil {
		pkg, err := pkger.Combine(pkgs, pkger.ValidSkipParseError())
		return pkg, false, err
	}

	stdinPkg, err := pkger.Parse(b.convertEncoding(), pkger.FromReader(b.in), pkger.ValidSkipParseError())
	if err != nil {
		return nil, true, err
	}
	pkg, err := pkger.Combine(append(pkgs, stdinPkg), pkger.ValidSkipParseError())
	return pkg, true, err
}

func (b *cmdPkgBuilder) inStdIn() (*os.File, error) {
	stdin, _ := b.in.(*os.File)
	if stdin != os.Stdin {
		return nil, errors.New("input not stdIn")
	}

	info, err := stdin.Stat()
	if err != nil {
		return nil, err
	}
	if (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
		return nil, errors.New("input not stdIn")
	}
	return stdin, nil
}

func (b *cmdPkgBuilder) readLines(r io.Reader) ([]string, error) {
	bb, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var stdinInput []string
	for _, bs := range bytes.Split(bb, []byte("\n")) {
		trimmed := bytes.TrimSpace(bs)
		if len(trimmed) == 0 {
			continue
		}
		stdinInput = append(stdinInput, string(trimmed))
	}
	return stdinInput, nil
}

func (b *cmdPkgBuilder) getInput(msg, defaultVal string) string {
	ui := &input.UI{
		Writer: b.w,
		Reader: b.in,
	}
	return getInput(ui, msg, defaultVal)
}

func (b *cmdPkgBuilder) convertURLEncoding(url string) pkger.Encoding {
	urlBase := path.Ext(url)
	switch {
	case strings.HasPrefix(urlBase, ".jsonnet"):
		return pkger.EncodingJsonnet
	case strings.HasPrefix(urlBase, ".json"):
		return pkger.EncodingJSON
	case strings.HasPrefix(urlBase, ".yml") || strings.HasPrefix(urlBase, ".yaml"):
		return pkger.EncodingYAML
	}
	return b.convertEncoding()
}

func (b *cmdPkgBuilder) convertFileEncoding(file string) pkger.Encoding {
	ext := filepath.Ext(file)
	switch {
	case strings.HasPrefix(ext, ".jsonnet"):
		return pkger.EncodingJsonnet
	case strings.HasPrefix(ext, ".json"):
		return pkger.EncodingJSON
	case strings.HasPrefix(ext, ".yml") || strings.HasPrefix(ext, ".yaml"):
		return pkger.EncodingYAML
	}

	return b.convertEncoding()
}

func (b *cmdPkgBuilder) convertEncoding() pkger.Encoding {
	switch {
	case b.encoding == "json":
		return pkger.EncodingJSON
	case b.encoding == "yml" || b.encoding == "yaml":
		return pkger.EncodingYAML
	case b.encoding == "jsonnet":
		return pkger.EncodingJsonnet
	default:
		return pkger.EncodingSource
	}
}

func newResourcesToClone(kind pkger.Kind, idStrs []string) (pkger.CreatePkgSetFn, error) {
	ids, err := toInfluxIDs(idStrs)
	if err != nil {
		return nil, err
	}

	var resources []pkger.ResourceToClone
	for _, id := range ids {
		resources = append(resources, pkger.ResourceToClone{
			Kind: kind,
			ID:   id,
		})
	}
	return pkger.CreateWithExistingResources(resources...), nil
}

func toInfluxIDs(args []string) ([]influxdb.ID, error) {
	var (
		ids  []influxdb.ID
		errs []string
	)
	for _, arg := range args {
		normedArg := strings.TrimSpace(strings.ToLower(arg))
		if normedArg == "" {
			continue
		}

		id, err := influxdb.IDFromString(normedArg)
		if err != nil {
			errs = append(errs, "arg must provide a valid 16 length ID; got: "+arg)
			continue
		}
		ids = append(ids, *id)
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "\n\t"))
	}

	return ids, nil
}

func createPkgBuf(pkg *pkger.Pkg, outPath string) (*bytes.Buffer, error) {
	var encoding pkger.Encoding
	switch ext := filepath.Ext(outPath); ext {
	case ".json":
		encoding = pkger.EncodingJSON
	default:
		encoding = pkger.EncodingYAML
	}

	b, err := pkg.Encode(encoding)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(b), nil
}

func newPkgerSVC() (pkger.SVC, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSvc := &ihttp.OrganizationService{
		Client: httpClient,
	}

	return &pkger.HTTPRemoteService{Client: httpClient}, orgSvc, nil
}

func (b *cmdPkgBuilder) printPkgDiff(diff pkger.Diff) error {
	if b.quiet {
		return nil
	}

	if b.json {
		return b.writeJSON(diff)
	}

	diffPrinterGen := func(title string, headers []string) *diffPrinter {
		commonHeaders := []string{"Package Name", "ID", "Resource Name"}

		printer := newDiffPrinter(b.w, !b.disableColor, !b.disableTableBorders)
		printer.
			Title(title).
			SetHeaders(append(commonHeaders, headers...)...)
		return printer
	}

	if labels := diff.Labels; len(labels) > 0 {
		printer := diffPrinterGen("Labels", []string{"Color", "Description"})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffLabelValues) []string {
			return []string{pkgName, id.String(), v.Name, v.Color, v.Description}
		}

		for _, l := range labels {
			var oldRow []string
			if l.Old != nil {
				oldRow = appendValues(l.ID, l.PkgName, *l.Old)
			}

			newRow := appendValues(l.ID, l.PkgName, l.New)
			switch {
			case l.IsNew():
				printer.AppendDiff(nil, newRow)
			case l.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if bkts := diff.Buckets; len(bkts) > 0 {
		printer := diffPrinterGen("Buckets", []string{"Retention Period", "Description"})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffBucketValues) []string {
			return []string{pkgName, id.String(), v.Name, v.RetentionRules.RP().String(), v.Description}
		}

		for _, b := range bkts {
			var oldRow []string
			if b.Old != nil {
				oldRow = appendValues(b.ID, b.PkgName, *b.Old)
			}

			newRow := appendValues(b.ID, b.PkgName, b.New)
			switch {
			case b.IsNew():
				printer.AppendDiff(nil, newRow)
			case b.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if checks := diff.Checks; len(checks) > 0 {
		printer := diffPrinterGen("Checks", []string{"Description"})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffCheckValues) []string {
			out := []string{pkgName, id.String()}
			if v.Check == nil {
				return append(out, "", "")
			}
			return append(out, v.Check.GetName(), v.Check.GetDescription())
		}

		for _, c := range checks {
			var oldRow []string
			if c.Old != nil {
				oldRow = appendValues(c.ID, c.PkgName, *c.Old)
			}

			newRow := appendValues(c.ID, c.PkgName, c.New)
			switch {
			case c.IsNew():
				printer.AppendDiff(nil, newRow)
			case c.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if dashes := diff.Dashboards; len(dashes) > 0 {
		printer := diffPrinterGen("Dashboards", []string{"Description", "Num Charts"})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffDashboardValues) []string {
			return []string{pkgName, id.String(), v.Name, v.Desc, strconv.Itoa(len(v.Charts))}
		}

		for _, d := range dashes {
			var oldRow []string
			if d.Old != nil {
				oldRow = appendValues(d.ID, d.PkgName, *d.Old)
			}

			newRow := appendValues(d.ID, d.PkgName, d.New)
			switch {
			case d.IsNew():
				printer.AppendDiff(nil, newRow)
			case d.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if endpoints := diff.NotificationEndpoints; len(endpoints) > 0 {
		printer := diffPrinterGen("Notification Endpoints", nil)

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffNotificationEndpointValues) []string {
			out := []string{pkgName, id.String()}
			if v.NotificationEndpoint == nil {
				return append(out, "")
			}
			return append(out, v.NotificationEndpoint.GetName())
		}

		for _, e := range endpoints {
			var oldRow []string
			if e.Old != nil {
				oldRow = appendValues(e.ID, e.PkgName, *e.Old)
			}

			newRow := appendValues(e.ID, e.PkgName, e.New)
			switch {
			case e.IsNew():
				printer.AppendDiff(nil, newRow)
			case e.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if rules := diff.NotificationRules; len(rules) > 0 {
		printer := diffPrinterGen("Notification Rules", []string{
			"Description",
			"Every",
			"Offset",
			"Endpoint Name",
			"Endpoint ID",
			"Endpoint Type",
		})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffNotificationRuleValues) []string {
			return []string{
				pkgName,
				id.String(),
				v.Name,
				v.Every,
				v.Offset,
				v.EndpointName,
				v.EndpointID.String(),
				v.EndpointType,
			}
		}

		for _, e := range rules {
			var oldRow []string
			if e.Old != nil {
				oldRow = appendValues(e.ID, e.PkgName, *e.Old)
			}

			newRow := appendValues(e.ID, e.PkgName, e.New)
			switch {
			case e.IsNew():
				printer.AppendDiff(nil, newRow)
			case e.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if teles := diff.Telegrafs; len(teles) > 0 {
		printer := diffPrinterGen("Telegraf Configurations", []string{"Description"})
		appendValues := func(id pkger.SafeID, pkgName string, v influxdb.TelegrafConfig) []string {
			return []string{pkgName, id.String(), v.Name, v.Description}
		}

		for _, e := range teles {
			var oldRow []string
			if e.Old != nil {
				oldRow = appendValues(e.ID, e.PkgName, *e.Old)
			}

			newRow := appendValues(e.ID, e.PkgName, e.New)
			switch {
			case e.IsNew():
				printer.AppendDiff(nil, newRow)
			case e.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if tasks := diff.Tasks; len(tasks) > 0 {
		printer := diffPrinterGen("Tasks", []string{"Description", "Cycle"})
		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffTaskValues) []string {
			timing := v.Cron
			if v.Cron == "" {
				timing = fmt.Sprintf("every: %s offset: %s", v.Every, v.Offset)
			}
			return []string{pkgName, id.String(), v.Name, v.Description, timing}
		}

		for _, e := range tasks {
			var oldRow []string
			if e.Old != nil {
				oldRow = appendValues(e.ID, e.PkgName, *e.Old)
			}

			newRow := appendValues(e.ID, e.PkgName, e.New)
			switch {
			case e.IsNew():
				printer.AppendDiff(nil, newRow)
			case e.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if vars := diff.Variables; len(vars) > 0 {
		printer := diffPrinterGen("Variables", []string{"Description", "Arg Type", "Arg Values"})

		appendValues := func(id pkger.SafeID, pkgName string, v pkger.DiffVariableValues) []string {
			var argType string
			if v.Args != nil {
				argType = v.Args.Type
			}
			return []string{pkgName, id.String(), v.Name, v.Description, argType, printVarArgs(v.Args)}
		}

		for _, v := range vars {
			var oldRow []string
			if v.Old != nil {
				oldRow = appendValues(v.ID, v.PkgName, *v.Old)
			}

			newRow := appendValues(v.ID, v.PkgName, v.New)
			switch {
			case v.IsNew():
				printer.AppendDiff(nil, newRow)
			case v.Remove:
				printer.AppendDiff(oldRow, nil)
			default:
				printer.AppendDiff(oldRow, newRow)
			}
		}
		printer.Render()
	}

	if len(diff.LabelMappings) > 0 {
		printer := newDiffPrinter(b.w, !b.disableColor, !b.disableTableBorders)
		printer.
			Title("Label Associations").
			SetHeaders(
				"Resource Type",
				"Resource Package Name", "Resource Name", "Resource ID",
				"Label Package Name", "Label Name", "Label ID",
			)

		for _, m := range diff.LabelMappings {
			newRow := []string{
				string(m.ResType),
				m.ResPkgName, m.ResName, m.ResID.String(),
				m.LabelPkgName, m.LabelName, m.LabelID.String(),
			}
			switch {
			case pkger.IsNew(m.StateStatus):
				printer.AppendDiff(nil, newRow)
			case pkger.IsRemoval(m.StateStatus):
				printer.AppendDiff(newRow, nil)
			default:
				printer.AppendDiff(newRow, newRow)
			}
		}
		printer.Render()
	}

	return nil
}

func (b *cmdPkgBuilder) printPkgSummary(sum pkger.Summary) error {
	if b.quiet {
		return nil
	}

	if b.json {
		return b.writeJSON(sum)
	}

	commonHeaders := []string{"Package Name", "ID", "Resource Name"}

	tablePrintFn := b.tablePrinterGen()
	if labels := sum.Labels; len(labels) > 0 {
		headers := append(commonHeaders, "Description", "Color")
		tablePrintFn("LABELS", headers, len(labels), func(i int) []string {
			l := labels[i]
			return []string{
				l.PkgName,
				l.ID.String(),
				l.Name,
				l.Properties.Description,
				l.Properties.Color,
			}
		})
	}

	if buckets := sum.Buckets; len(buckets) > 0 {
		headers := append(commonHeaders, "Retention", "Description")
		tablePrintFn("BUCKETS", headers, len(buckets), func(i int) []string {
			bucket := buckets[i]
			return []string{
				bucket.PkgName,
				bucket.ID.String(),
				bucket.Name,
				formatDuration(bucket.RetentionPeriod),
				bucket.Description,
			}
		})
	}

	if checks := sum.Checks; len(checks) > 0 {
		headers := append(commonHeaders, "Description")
		tablePrintFn("CHECKS", headers, len(checks), func(i int) []string {
			c := checks[i].Check
			return []string{
				checks[i].PkgName,
				c.GetID().String(),
				c.GetName(),
				c.GetDescription(),
			}
		})
	}

	if dashes := sum.Dashboards; len(dashes) > 0 {
		headers := append(commonHeaders, "Description")
		tablePrintFn("DASHBOARDS", headers, len(dashes), func(i int) []string {
			d := dashes[i]
			return []string{d.PkgName, d.ID.String(), d.Name, d.Description}
		})
	}

	if endpoints := sum.NotificationEndpoints; len(endpoints) > 0 {
		headers := append(commonHeaders, "Description", "Status")
		tablePrintFn("NOTIFICATION ENDPOINTS", headers, len(endpoints), func(i int) []string {
			v := endpoints[i]
			return []string{
				v.PkgName,
				v.NotificationEndpoint.GetID().String(),
				v.NotificationEndpoint.GetName(),
				v.NotificationEndpoint.GetDescription(),
				string(v.NotificationEndpoint.GetStatus()),
			}
		})
	}

	if rules := sum.NotificationRules; len(rules) > 0 {
		headers := append(commonHeaders, "Description", "Every", "Offset", "Endpoint Name", "Endpoint ID", "Endpoint Type")
		tablePrintFn("NOTIFICATION RULES", headers, len(rules), func(i int) []string {
			v := rules[i]
			return []string{
				v.PkgName,
				v.ID.String(),
				v.Name,
				v.Description,
				v.Every,
				v.Offset,
				v.EndpointPkgName,
				v.EndpointID.String(),
				v.EndpointType,
			}
		})
	}

	if tasks := sum.Tasks; len(tasks) > 0 {
		headers := append(commonHeaders, "Description", "Cycle")
		tablePrintFn("TASKS", headers, len(tasks), func(i int) []string {
			t := tasks[i]
			timing := fmt.Sprintf("every: %s offset: %s", t.Every, t.Offset)
			if t.Cron != "" {
				timing = t.Cron
			}
			return []string{
				t.PkgName,
				t.ID.String(),
				t.Name,
				t.Description,
				timing,
			}
		})
	}

	if teles := sum.TelegrafConfigs; len(teles) > 0 {
		headers := append(commonHeaders, "Description")
		tablePrintFn("TELEGRAF CONFIGS", headers, len(teles), func(i int) []string {
			t := teles[i]
			return []string{
				t.PkgName,
				t.TelegrafConfig.ID.String(),
				t.TelegrafConfig.Name,
				t.TelegrafConfig.Description,
			}
		})
	}

	if vars := sum.Variables; len(vars) > 0 {
		headers := append(commonHeaders, "Description", "Arg Type", "Arg Values")
		tablePrintFn("VARIABLES", headers, len(vars), func(i int) []string {
			v := vars[i]
			args := v.Arguments
			return []string{
				v.PkgName,
				v.ID.String(),
				v.Name,
				v.Description,
				args.Type,
				printVarArgs(args),
			}
		})
	}

	if mappings := sum.LabelMappings; len(mappings) > 0 {
		headers := []string{"Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrintFn("LABEL ASSOCIATIONS", headers, len(mappings), func(i int) []string {
			m := mappings[i]
			return []string{
				string(m.ResourceType),
				m.ResourceName,
				m.ResourceID.String(),
				m.LabelName,
				m.LabelID.String(),
			}
		})
	}

	if secrets := sum.MissingSecrets; len(secrets) > 0 {
		headers := []string{"Secret Key"}
		tablePrintFn("MISSING SECRETS", headers, len(secrets), func(i int) []string {
			return []string{secrets[i]}
		})
	}

	return nil
}

func (b *cmdPkgBuilder) tablePrinterGen() func(table string, headers []string, count int, rowFn func(i int) []string) {
	return func(table string, headers []string, count int, rowFn func(i int) []string) {
		tablePrinter(b.w, table, headers, count, !b.disableColor, !b.disableTableBorders, rowFn)
	}
}

type diffPrinter struct {
	w      io.Writer
	writer *tablewriter.Table

	colorAdd     tablewriter.Colors
	colorFooter  tablewriter.Colors
	colorHeaders tablewriter.Colors
	colorRemove  tablewriter.Colors
	title        string

	appendCalls int
	headerLen   int
}

func newDiffPrinter(w io.Writer, hasColor, hasBorder bool) *diffPrinter {
	wr := tablewriter.NewWriter(w)
	wr.SetBorder(hasBorder)
	wr.SetRowLine(hasBorder)

	var (
		colorAdd    = tablewriter.Colors{}
		colorFooter = tablewriter.Colors{}
		colorHeader = tablewriter.Colors{}
		colorRemove = tablewriter.Colors{}
	)
	if hasColor {
		colorAdd = tablewriter.Colors{tablewriter.FgHiGreenColor, tablewriter.Bold}
		colorFooter = tablewriter.Color(tablewriter.FgHiBlueColor, tablewriter.Bold)
		colorHeader = tablewriter.Colors{tablewriter.FgCyanColor, tablewriter.Bold}
		colorRemove = tablewriter.Colors{tablewriter.FgRedColor, tablewriter.Bold}
	}
	return &diffPrinter{
		w:            w,
		writer:       wr,
		colorAdd:     colorAdd,
		colorFooter:  colorFooter,
		colorHeaders: colorHeader,
		colorRemove:  colorRemove,
	}
}

func (d *diffPrinter) Render() {
	if d.appendCalls == 0 {
		return
	}

	// set the title and the add/remove legend
	title := color.New(color.FgYellow, color.Bold).Sprint(strings.ToUpper(d.title))
	add := color.New(color.FgHiGreen, color.Bold).Sprint("+add")
	remove := color.New(color.FgRed, color.Bold).Sprint("-remove")
	fmt.Fprintf(d.w, "%s    %s | %s | unchanged\n", title, add, remove)

	d.setFooter()
	d.writer.Render()
	fmt.Fprintln(d.w)
}

func (d *diffPrinter) Title(title string) *diffPrinter {
	d.title = title
	return d
}

func (d *diffPrinter) SetHeaders(headers ...string) *diffPrinter {
	headers = d.prepend(headers, "+/-")
	d.headerLen = len(headers)

	d.writer.SetHeader(headers)

	headerColors := make([]tablewriter.Colors, d.headerLen)
	for i := range headerColors {
		headerColors[i] = d.colorHeaders
	}
	d.writer.SetHeaderColor(headerColors...)

	return d
}

func (d *diffPrinter) setFooter() *diffPrinter {
	footers := make([]string, d.headerLen)
	if d.headerLen > 1 {
		footers[len(footers)-2] = "TOTAL"
		footers[len(footers)-1] = strconv.Itoa(d.appendCalls)
	} else {
		footers[0] = "TOTAL: " + strconv.Itoa(d.appendCalls)
	}

	d.writer.SetFooter(footers)
	colors := make([]tablewriter.Colors, d.headerLen)
	if d.headerLen > 1 {
		colors[len(colors)-2] = d.colorFooter
		colors[len(colors)-1] = d.colorFooter
	} else {
		colors[0] = d.colorFooter
	}
	d.writer.SetFooterColor(colors...)

	return d
}

func (d *diffPrinter) Append(slc []string) {
	d.writer.Append(d.prepend(slc, ""))
}

func (d *diffPrinter) AppendDiff(remove, add []string) {
	defer func() { d.appendCalls++ }()

	if d.appendCalls > 0 {
		d.appendBufferLine()
	}

	lenAdd, lenRemove := len(add), len(remove)
	preppedAdd, preppedRemove := d.prepend(add, "+"), d.prepend(remove, "-")
	if lenRemove > 0 && lenAdd == 0 {
		d.writer.Rich(preppedRemove, d.redRow(len(preppedRemove)))
		return
	}
	if lenAdd > 0 && lenRemove == 0 {
		d.writer.Rich(preppedAdd, d.greenRow(len(preppedAdd)))
		return
	}

	var (
		addColors    = make([]tablewriter.Colors, len(preppedAdd))
		removeColors = make([]tablewriter.Colors, len(preppedRemove))
		hasDiff      bool
	)
	for i := 0; i < lenRemove; i++ {
		if add[i] != remove[i] {
			hasDiff = true
			// offset to skip prepended +/- column
			addColors[i+1], removeColors[i+1] = d.colorAdd, d.colorRemove
		}
	}

	if !hasDiff {
		d.writer.Append(d.prepend(add, ""))
		return
	}

	addColors[0], removeColors[0] = d.colorAdd, d.colorRemove
	d.writer.Rich(d.prepend(remove, "-"), removeColors)
	d.writer.Rich(d.prepend(add, "+"), addColors)
}

func (d *diffPrinter) appendBufferLine() {
	d.writer.Append([]string{})
}

func (d *diffPrinter) redRow(i int) []tablewriter.Colors {
	return colorRow(d.colorRemove, i)
}

func (d *diffPrinter) greenRow(i int) []tablewriter.Colors {
	return colorRow(d.colorAdd, i)
}

func (d *diffPrinter) prepend(slc []string, val string) []string {
	return append([]string{val}, slc...)
}

func colorRow(color tablewriter.Colors, i int) []tablewriter.Colors {
	colors := make([]tablewriter.Colors, i)
	for i := range colors {
		colors[i] = color
	}
	return colors
}

func tablePrinter(wr io.Writer, table string, headers []string, count int, hasColor, hasTableBorders bool, rowFn func(i int) []string) {
	color.New(color.FgYellow, color.Bold).Fprintln(wr, strings.ToUpper(table))

	w := tablewriter.NewWriter(wr)
	w.SetBorder(hasTableBorders)
	w.SetRowLine(hasTableBorders)

	var alignments []int
	for range headers {
		alignments = append(alignments, tablewriter.ALIGN_CENTER)
	}

	descrCol := find("description", headers)
	if descrCol != -1 {
		w.SetColMinWidth(descrCol, 30)
		alignments[descrCol] = tablewriter.ALIGN_LEFT
	}

	w.SetHeader(headers)
	w.SetColumnAlignment(alignments)

	for i := range make([]struct{}, count) {
		w.Append(rowFn(i))
	}

	footers := make([]string, len(headers))
	if len(headers) > 1 {
		footers[len(footers)-2] = "TOTAL"
		footers[len(footers)-1] = strconv.Itoa(count)
	} else {
		footers[0] = "TOTAL: " + strconv.Itoa(count)
	}
	w.SetFooter(footers)
	if hasColor {
		headerColor := tablewriter.Color(tablewriter.FgHiCyanColor, tablewriter.Bold)
		footerColor := tablewriter.Color(tablewriter.FgHiBlueColor, tablewriter.Bold)

		var colors []tablewriter.Colors
		for i := 0; i < len(headers); i++ {
			colors = append(colors, headerColor)
		}
		w.SetHeaderColor(colors...)
		if len(headers) > 1 {
			colors[len(colors)-2] = footerColor
			colors[len(colors)-1] = footerColor
		} else {
			colors[0] = footerColor
		}
		w.SetFooterColor(colors...)
	}

	w.Render()
	fmt.Fprintln(wr)
}

func printVarArgs(a *influxdb.VariableArguments) string {
	if a == nil {
		return "<nil>"
	}
	if a.Type == "map" {
		b, err := json.Marshal(a.Values)
		if err != nil {
			return "{}"
		}
		return string(b)
	}
	if a.Type == "constant" {
		vals, ok := a.Values.(influxdb.VariableConstantValues)
		if !ok {
			return "[]"
		}
		var out []string
		for _, s := range vals {
			out = append(out, fmt.Sprintf("%q", s))
		}
		return fmt.Sprintf("[%s]", strings.Join(out, " "))
	}
	if a.Type == "query" {
		qVal, ok := a.Values.(influxdb.VariableQueryValues)
		if !ok {
			return ""
		}
		return fmt.Sprintf("language=%q query=%q", qVal.Language, qVal.Query)
	}
	return "unknown variable argument"
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "inf"
	}
	return d.String()
}

func readFilesFromPath(filePath string, recurse bool) ([]string, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{filePath}, nil
	}

	dirFiles, err := ioutil.ReadDir(filePath)
	if err != nil {
		return nil, err
	}

	mFiles := make(map[string]struct{})
	assign := func(ss ...string) {
		for _, s := range ss {
			mFiles[s] = struct{}{}
		}
	}
	for _, f := range dirFiles {
		fileP := filepath.Join(filePath, f.Name())
		if f.IsDir() {
			if recurse {
				rFiles, err := readFilesFromPath(fileP, recurse)
				if err != nil {
					return nil, err
				}
				assign(rFiles...)
			}
			continue
		}
		assign(fileP)
	}

	var files []string
	for f := range mFiles {
		files = append(files, f)
	}
	return files, nil
}

func mapKeys(provided, kvPairs []string) map[string]string {
	out := make(map[string]string)
	for _, k := range provided {
		out[k] = ""
	}

	for _, pair := range kvPairs {
		pieces := strings.SplitN(pair, "=", 2)
		if len(pieces) < 2 {
			continue
		}

		k, v := pieces[0], pieces[1]
		if _, ok := out[k]; !ok {
			continue
		}
		out[k] = v
	}

	return out
}

func missingValKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v != "" {
			continue
		}
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i] < out[j]
	})
	return out
}

func find(needle string, haystack []string) int {
	for i, h := range haystack {
		if strings.ToLower(h) == needle {
			return i
		}
	}
	return -1
}
