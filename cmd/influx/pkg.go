package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/influxdata/influxdb"
	ihttp "github.com/influxdata/influxdb/http"
	ierror "github.com/influxdata/influxdb/kit/errors"
	"github.com/influxdata/influxdb/pkger"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	input "github.com/tcnksm/go-input"
	"gopkg.in/yaml.v3"
)

type pkgSVCsFn func() (pkger.SVC, influxdb.OrganizationService, error)

func cmdPkg(svcFn pkgSVCsFn, opts ...genericCLIOptfn) *cobra.Command {
	return newCmdPkgBuilder(svcFn, opts...).cmdPkg()
}

type cmdPkgBuilder struct {
	genericCLIOpts

	svcFn pkgSVCsFn

	file            string
	hasColor        bool
	hasTableBorders bool
	meta            pkger.Metadata
	org             organization
	quiet           bool

	applyOpts struct {
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

func newCmdPkgBuilder(svcFn pkgSVCsFn, opts ...genericCLIOptfn) *cmdPkgBuilder {
	opt := genericCLIOpts{
		in: os.Stdin,
		w:  os.Stdout,
	}
	for _, o := range opts {
		o(&opt)
	}

	return &cmdPkgBuilder{
		genericCLIOpts: opt,
		svcFn:          svcFn,
	}
}

func (b *cmdPkgBuilder) cmdPkg() *cobra.Command {
	cmd := b.cmdPkgApply()
	cmd.AddCommand(
		b.cmdPkgNew(),
		b.cmdPkgExport(),
		b.cmdPkgSummary(),
		b.cmdPkgValidate(),
	)
	return cmd
}

func (b *cmdPkgBuilder) cmdPkgApply() *cobra.Command {
	cmd := b.newCmd("pkg")
	cmd.Short = "Apply a pkg to create resources"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "Path to package file")
	cmd.MarkFlagFilename("file", "yaml", "yml", "json")
	cmd.Flags().BoolVarP(&b.quiet, "quiet", "q", false, "disable output printing")
	cmd.Flags().StringVar(&b.applyOpts.force, "force", "", `TTY input, if package will have destructive changes, proceed if set "true"`)

	b.org.register(cmd)

	cmd.Flags().BoolVarP(&b.hasColor, "color", "c", true, "Enable color in output, defaults true")
	cmd.Flags().BoolVar(&b.hasTableBorders, "table-borders", true, "Enable table borders, defaults true")

	b.applyOpts.secrets = []string{}
	cmd.Flags().StringSliceVar(&b.applyOpts.secrets, "secret", nil, "Secrets to provide alongside the package; format should --secret=SECRET_KEY::SECRET_VALUE --secret=SECRET_KEY_2::SECRET_VALUE_2")

	cmd.RunE = b.pkgApplyRunEFn()

	return cmd
}

func (b *cmdPkgBuilder) pkgApplyRunEFn() func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (e error) {
		if err := b.org.validOrgFlags(); err != nil {
			return err
		}
		color.NoColor = !b.hasColor

		svc, orgSVC, err := b.svcFn()
		if err != nil {
			return err
		}

		if err := b.org.validOrgFlags(); err != nil {
			return err
		}

		influxOrgID, err := b.org.getID(orgSVC)
		if err != nil {
			return nil
		}

		pkg, isTTY, err := b.readPkgStdInOrFile(b.file)
		if err != nil {
			return err
		}

		drySum, diff, err := svc.DryRun(context.Background(), influxOrgID, 0, pkg)
		if err != nil {
			return err
		}

		providedSecrets := make(map[string]string)
		for _, secretKey := range drySum.MissingSecrets {
			providedSecrets[secretKey] = ""
		}
		for _, secretPair := range b.applyOpts.secrets {
			pieces := strings.Split(secretPair, "::")
			if len(pieces) < 2 {
				continue
			}
			providedSecrets[pieces[0]] = pieces[1]
		}

		if !isTTY {
			for secretKey, existinVal := range providedSecrets {
				if existinVal != "" {
					continue
				}
				ui := &input.UI{
					Writer: os.Stdout,
					Reader: os.Stdin,
				}

				const skipDefault = "skip-this-key"
				prompt := "Please provide secret value for key " + secretKey + " (optional, press enter to skip)"
				secretVal := getInput(ui, prompt, skipDefault)
				if secretVal != "" && secretVal != skipDefault {
					providedSecrets[secretKey] = secretVal
				}
			}
		}

		if !b.quiet {
			b.printPkgDiff(diff)
		}

		isForced, _ := strconv.ParseBool(b.applyOpts.force)
		if !isTTY && !isForced && b.applyOpts.force != "conflict" {
			ui := &input.UI{
				Writer: os.Stdout,
				Reader: os.Stdin,
			}

			confirm := getInput(ui, "Confirm application of the above resources (y/n)", "n")
			if strings.ToLower(confirm) != "y" {
				fmt.Fprintln(os.Stdout, "aborted application of package")
				return nil
			}
		}

		if b.applyOpts.force != "conflict" && isTTY && diff.HasConflicts() {
			return errors.New("package has conflicts with existing resources and cannot safely apply")
		}

		summary, err := svc.Apply(context.Background(), influxOrgID, 0, pkg, pkger.ApplyWithSecrets(providedSecrets))
		if err != nil {
			return err
		}

		if !b.quiet {
			b.printPkgSummary(summary)
		}

		return nil
	}
}

func (b *cmdPkgBuilder) cmdPkgNew() *cobra.Command {
	cmd := b.newCmd("new")
	cmd.Short = "Create a reusable pkg to create resources in a declarative manner"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().BoolVarP(&b.quiet, "quiet", "q", false, "skip interactive mode")
	cmd.Flags().StringVarP(&b.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&b.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&b.meta.Version, "version", "v", "", "version for new pkg")

	cmd.RunE = b.pkgNewRunEFn()

	return cmd
}

func (b *cmdPkgBuilder) pkgNewRunEFn() func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if !b.quiet {
			ui := &input.UI{
				Writer: b.w,
				Reader: b.in,
			}

			if b.meta.Name == "" {
				b.meta.Name = getInput(ui, "pkg name", "")
			}
			if b.meta.Description == "" {
				b.meta.Description = getInput(ui, "pkg description", "")
			}
			if b.meta.Version == "" {
				b.meta.Version = getInput(ui, "pkg version", "")
			}
		}

		pkgSVC, _, err := b.svcFn()
		if err != nil {
			return err
		}

		return b.writePkg(cmd.OutOrStdout(), pkgSVC, b.file, pkger.CreateWithMetadata(b.meta))
	}
}

func (b *cmdPkgBuilder) cmdPkgExport() *cobra.Command {
	cmd := b.newCmd("export")
	cmd.Short = "Export existing resources as a package"
	cmd.AddCommand(b.cmdPkgExportAll())

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().StringVarP(&b.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&b.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&b.meta.Version, "version", "v", "", "version for new pkg")
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

	cmd.RunE = b.pkgExportRunEFn()

	return cmd
}

func (b *cmdPkgBuilder) pkgExportRunEFn() func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		pkgSVC, _, err := b.svcFn()
		if err != nil {
			return err
		}

		opts := []pkger.CreatePkgSetFn{pkger.CreateWithMetadata(b.meta)}

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

		kind := pkger.NewKind(b.exportOpts.resourceType)
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
}

func (b *cmdPkgBuilder) cmdPkgExportAll() *cobra.Command {
	cmd := b.newCmd("all")
	cmd.Short = "Export all existing resources for an organization as a package"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")

	b.org.register(cmd)

	cmd.Flags().StringVarP(&b.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&b.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&b.meta.Version, "version", "v", "", "version for new pkg")

	cmd.RunE = b.pkgExportAllRunEFn()

	return cmd
}

func (b *cmdPkgBuilder) pkgExportAllRunEFn() func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		pkgSVC, orgSVC, err := b.svcFn()
		if err != nil {
			return err
		}

		opts := []pkger.CreatePkgSetFn{pkger.CreateWithMetadata(b.meta)}

		orgID, err := b.org.getID(orgSVC)
		if err != nil {
			return err
		}
		opts = append(opts, pkger.CreateWithAllOrgResources(orgID))

		return b.writePkg(cmd.OutOrStdout(), pkgSVC, b.file, opts...)
	}
}

func (b *cmdPkgBuilder) cmdPkgSummary() *cobra.Command {
	cmd := b.newCmd("summary")
	cmd.Short = "Summarize the provided package"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "input file for pkg; if none provided will use TTY input")
	cmd.Flags().BoolVarP(&b.hasColor, "color", "c", true, "Enable color in output, defaults true")
	cmd.Flags().BoolVar(&b.hasTableBorders, "table-borders", true, "Enable table borders, defaults true")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		pkg, _, err := b.readPkgStdInOrFile(b.file)
		if err != nil {
			return err
		}

		b.printPkgSummary(pkg.Summary())
		return nil
	}

	return cmd
}

func (b *cmdPkgBuilder) cmdPkgValidate() *cobra.Command {
	cmd := b.newCmd("validate")
	cmd.Short = "Validate the provided package"

	cmd.Flags().StringVarP(&b.file, "file", "f", "", "input file for pkg; if none provided will use TTY input")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		pkg, _, err := b.readPkgStdInOrFile(b.file)
		if err != nil {
			return err
		}
		return pkg.Validate()
	}

	return cmd
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

func (b *cmdPkgBuilder) readPkgStdInOrFile(file string) (*pkger.Pkg, bool, error) {
	if file != "" {
		pkg, err := pkgFromFile(file)
		return pkg, false, err
	}

	var isTTY bool

	if _, err := b.inStdIn(); err == nil {
		isTTY = true
	}

	pkg, err := pkgFromReader(b.in)
	return pkg, isTTY, err
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
	var (
		buf bytes.Buffer
		enc interface {
			Encode(interface{}) error
		}
	)

	switch ext := filepath.Ext(outPath); ext {
	case ".json":
		jsonEnc := json.NewEncoder(&buf)
		jsonEnc.SetIndent("", "\t")
		enc = jsonEnc
	default:
		enc = yaml.NewEncoder(&buf)
	}
	if err := enc.Encode(pkg); err != nil {
		return nil, err
	}

	return &buf, nil
}

func newPkgerSVC() (pkger.SVC, influxdb.OrganizationService, error) {
	httpClient, err := newHTTPClient()
	if err != nil {
		return nil, nil, err
	}

	orgSvc := &ihttp.OrganizationService{
		Client: httpClient,
	}

	return &ihttp.PkgerService{Client: httpClient}, orgSvc, nil
}

func pkgFromReader(stdin io.Reader) (*pkger.Pkg, error) {
	b, err := ioutil.ReadAll(stdin)
	if err != nil {
		return nil, err
	}

	var enc pkger.Encoding
	switch http.DetectContentType(b[0:512]) {
	case "application/json":
		enc = pkger.EncodingJSON
	default:
		enc = pkger.EncodingYAML
	}
	return pkger.Parse(enc, pkger.FromString(string(b)))
}

func pkgFromFile(path string) (*pkger.Pkg, error) {
	var enc pkger.Encoding
	switch ext := filepath.Ext(path); ext {
	case ".yaml", ".yml":
		enc = pkger.EncodingYAML
	case ".json":
		enc = pkger.EncodingJSON
	default:
		return nil, errors.New("file provided must be one of yaml/yml/json extension but got: " + ext)
	}

	return pkger.Parse(enc, pkger.FromFile(path))
}

func (b *cmdPkgBuilder) printPkgDiff(diff pkger.Diff) {
	red := color.New(color.FgRed).SprintFunc()
	green := color.New(color.FgHiGreen, color.Bold).SprintFunc()

	diffLn := func(isNew bool, old, new interface{}) string {
		if isNew {
			return green(new)
		}
		if reflect.DeepEqual(old, new) {
			return fmt.Sprint(new)
		}
		return fmt.Sprintf("%s\n%s", red(old), green(new))
	}

	boolDiff := func(b bool) string {
		bb := strconv.FormatBool(b)
		if b {
			return green(bb)
		}
		return bb
	}

	tablePrintFn := b.tablePrinterGen()
	if labels := diff.Labels; len(labels) > 0 {
		headers := []string{"New", "ID", "Name", "Color", "Description"}
		tablePrintFn("LABELS", headers, len(labels), func(i int) []string {
			l := labels[i]
			var old pkger.DiffLabelValues
			if l.Old != nil {
				old = *l.Old
			}

			return []string{
				boolDiff(l.IsNew()),
				l.ID.String(),
				l.Name,
				diffLn(l.IsNew(), old.Color, l.New.Color),
				diffLn(l.IsNew(), old.Description, l.New.Description),
			}
		})
	}

	if bkts := diff.Buckets; len(bkts) > 0 {
		headers := []string{"New", "ID", "Name", "Retention Period", "Description"}
		tablePrintFn("BUCKETS", headers, len(bkts), func(i int) []string {
			b := bkts[i]
			var old pkger.DiffBucketValues
			if b.Old != nil {
				old = *b.Old
			}
			return []string{
				boolDiff(b.IsNew()),
				b.ID.String(),
				b.Name,
				diffLn(b.IsNew(), old.RetentionRules.RP().String(), b.New.RetentionRules.RP().String()),
				diffLn(b.IsNew(), old.Description, b.New.Description),
			}
		})
	}

	if checks := diff.Checks; len(checks) > 0 {
		headers := []string{"New", "ID", "Name", "Description"}
		tablePrintFn("CHECKS", headers, len(checks), func(i int) []string {
			c := checks[i]
			var oldDesc string
			if c.Old != nil {
				oldDesc = c.Old.GetDescription()
			}
			return []string{
				boolDiff(c.IsNew()),
				c.ID.String(),
				c.Name,
				diffLn(c.IsNew(), oldDesc, c.New.GetDescription()),
			}
		})
	}

	if dashes := diff.Dashboards; len(dashes) > 0 {
		headers := []string{"New", "Name", "Description", "Num Charts"}
		tablePrintFn("DASHBOARDS", headers, len(dashes), func(i int) []string {
			d := dashes[i]
			return []string{
				boolDiff(true),
				d.Name,
				green(d.Desc),
				green(strconv.Itoa(len(d.Charts))),
			}
		})
	}

	if endpoints := diff.NotificationEndpoints; len(endpoints) > 0 {
		headers := []string{"New", "ID", "Name"}
		tablePrintFn("NOTIFICATION ENDPOINTS", headers, len(endpoints), func(i int) []string {
			v := endpoints[i]
			return []string{
				boolDiff(v.IsNew()),
				v.ID.String(),
				v.Name,
			}
		})
	}

	if rules := diff.NotificationRules; len(rules) > 0 {
		headers := []string{"New", "Name", "Description", "Every", "Offset", "Endpoint Name", "Endpoint ID", "Endpoint Type"}
		tablePrintFn("NOTIFICATION RULES", headers, len(rules), func(i int) []string {
			v := rules[i]
			return []string{
				green(true),
				v.Name,
				v.Description,
				v.Every,
				v.Offset,
				v.EndpointName,
				v.EndpointID.String(),
				v.EndpointType,
			}
		})
	}

	if teles := diff.Telegrafs; len(teles) > 0 {
		headers := []string{"New", "Name", "Description"}
		tablePrintFn("TELEGRAF CONFIGS", headers, len(teles), func(i int) []string {
			t := teles[i]
			return []string{
				boolDiff(true),
				t.Name,
				green(t.Description),
			}
		})
	}

	if tasks := diff.Tasks; len(tasks) > 0 {
		headers := []string{"New", "Name", "Description", "Cycle"}
		tablePrintFn("TASKS", headers, len(tasks), func(i int) []string {
			t := tasks[i]
			timing := fmt.Sprintf("every: %s offset: %s", t.Every, t.Offset)
			if t.Cron != "" {
				timing = t.Cron
			}
			return []string{
				boolDiff(true),
				t.Name,
				green(t.Description),
				green(timing),
			}
		})
	}

	if vars := diff.Variables; len(vars) > 0 {
		headers := []string{"New", "ID", "Name", "Description", "Arg Type", "Arg Values"}
		tablePrintFn("VARIABLES", headers, len(vars), func(i int) []string {
			v := vars[i]
			var old pkger.DiffVariableValues
			if v.Old != nil {
				old = *v.Old
			}
			var oldArgType string
			if old.Args != nil {
				oldArgType = old.Args.Type
			}
			var newArgType string
			if v.New.Args != nil {
				newArgType = v.New.Args.Type
			}
			return []string{
				boolDiff(v.IsNew()),
				v.ID.String(),
				v.Name,
				diffLn(v.IsNew(), old.Description, v.New.Description),
				diffLn(v.IsNew(), oldArgType, newArgType),
				diffLn(v.IsNew(), printVarArgs(old.Args), printVarArgs(v.New.Args)),
			}
		})
	}

	if len(diff.LabelMappings) > 0 {
		headers := []string{"New", "Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrintFn("LABEL MAPPINGS", headers, len(diff.LabelMappings), func(i int) []string {
			m := diff.LabelMappings[i]
			return []string{
				boolDiff(m.IsNew),
				string(m.ResType),
				m.ResName,
				m.ResID.String(),
				m.LabelName,
				m.LabelID.String(),
			}
		})
	}
}

func (b *cmdPkgBuilder) printPkgSummary(sum pkger.Summary) {
	tablePrintFn := b.tablePrinterGen()
	if labels := sum.Labels; len(labels) > 0 {
		headers := []string{"ID", "Name", "Description", "Color"}
		tablePrintFn("LABELS", headers, len(labels), func(i int) []string {
			l := labels[i]
			return []string{
				l.ID.String(),
				l.Name,
				l.Properties.Description,
				l.Properties.Color,
			}
		})
	}

	if buckets := sum.Buckets; len(buckets) > 0 {
		headers := []string{"ID", "Name", "Retention", "Description"}
		tablePrintFn("BUCKETS", headers, len(buckets), func(i int) []string {
			bucket := buckets[i]
			return []string{
				bucket.ID.String(),
				bucket.Name,
				formatDuration(bucket.RetentionPeriod),
				bucket.Description,
			}
		})
	}

	if checks := sum.Checks; len(checks) > 0 {
		headers := []string{"ID", "Name", "Description"}
		tablePrintFn("CHECKS", headers, len(checks), func(i int) []string {
			c := checks[i].Check
			return []string{
				c.GetID().String(),
				c.GetName(),
				c.GetDescription(),
			}
		})
	}

	if dashes := sum.Dashboards; len(dashes) > 0 {
		headers := []string{"ID", "Name", "Description"}
		tablePrintFn("DASHBOARDS", headers, len(dashes), func(i int) []string {
			d := dashes[i]
			return []string{d.ID.String(), d.Name, d.Description}
		})
	}

	if endpoints := sum.NotificationEndpoints; len(endpoints) > 0 {
		headers := []string{"ID", "Name", "Description", "Status"}
		tablePrintFn("NOTIFICATION ENDPOINTS", headers, len(endpoints), func(i int) []string {
			v := endpoints[i]
			return []string{
				v.NotificationEndpoint.GetID().String(),
				v.NotificationEndpoint.GetName(),
				v.NotificationEndpoint.GetDescription(),
				string(v.NotificationEndpoint.GetStatus()),
			}
		})
	}

	if rules := sum.NotificationRules; len(rules) > 0 {
		headers := []string{"ID", "Name", "Description", "Every", "Offset", "Endpoint Name", "Endpoint ID", "Endpoint Type"}
		tablePrintFn("NOTIFICATION RULES", headers, len(rules), func(i int) []string {
			v := rules[i]
			return []string{
				v.ID.String(),
				v.Name,
				v.Description,
				v.Every,
				v.Offset,
				v.EndpointName,
				v.EndpointID.String(),
				v.EndpointType,
			}
		})
	}

	if tasks := sum.Tasks; len(tasks) > 0 {
		headers := []string{"ID", "Name", "Description", "Cycle"}
		tablePrintFn("TASKS", headers, len(tasks), func(i int) []string {
			t := tasks[i]
			timing := fmt.Sprintf("every: %s offset: %s", t.Every, t.Offset)
			if t.Cron != "" {
				timing = t.Cron
			}
			return []string{
				t.ID.String(),
				t.Name,
				t.Description,
				timing,
			}
		})
	}

	if teles := sum.TelegrafConfigs; len(teles) > 0 {
		headers := []string{"ID", "Name", "Description"}
		tablePrintFn("TELEGRAF CONFIGS", headers, len(teles), func(i int) []string {
			t := teles[i]
			return []string{
				t.TelegrafConfig.ID.String(),
				t.TelegrafConfig.Name,
				t.TelegrafConfig.Description,
			}
		})
	}

	if vars := sum.Variables; len(vars) > 0 {
		headers := []string{"ID", "Name", "Description", "Arg Type", "Arg Values"}
		tablePrintFn("VARIABLES", headers, len(vars), func(i int) []string {
			v := vars[i]
			args := v.Arguments
			return []string{
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
		tablePrintFn("LABEL MAPPINGS", headers, len(mappings), func(i int) []string {
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
}

func (b *cmdPkgBuilder) tablePrinterGen() func(table string, headers []string, count int, rowFn func(i int) []string) {
	return func(table string, headers []string, count int, rowFn func(i int) []string) {
		tablePrinter(b.w, table, headers, count, b.hasColor, b.hasTableBorders, rowFn)
	}
}

func tablePrinter(wr io.Writer, table string, headers []string, count int, hasColor, hasTableBorders bool, rowFn func(i int) []string) {
	color.New(color.FgYellow, color.Bold).Fprintln(os.Stdout, strings.ToUpper(table))

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
		var colors []tablewriter.Colors
		for i := 0; i < len(headers); i++ {
			colors = append(colors, tablewriter.Color(tablewriter.FgHiCyanColor))
		}
		w.SetHeaderColor(colors...)
		if len(headers) > 1 {
			colors[len(colors)-2] = tablewriter.Color(tablewriter.FgHiBlueColor)
			colors[len(colors)-1] = tablewriter.Color(tablewriter.FgHiBlueColor)
		} else {
			colors[0] = tablewriter.Color(tablewriter.FgHiBlueColor)
		}
		w.SetFooterColor(colors...)
	}

	w.Render()
	fmt.Fprintln(os.Stdout)
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

func find(needle string, haystack []string) int {
	for i, h := range haystack {
		if strings.ToLower(h) == needle {
			return i
		}
	}
	return -1
}
