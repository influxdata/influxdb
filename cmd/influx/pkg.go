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

type pkgSVCFn func(cliReq httpClientOpts) (pkger.SVC, error)

func pkgCmd(newSVCFn pkgSVCFn) *cobra.Command {
	cmd := pkgApplyCmd(newSVCFn)
	cmd.AddCommand(
		pkgNewCmd(newSVCFn),
		pkgExportCmd(newSVCFn),
	)
	return cmd
}

type pkgApplyOpts struct {
	orgID, file               string
	hasColor, hasTableBorders bool
	quiet, forceOnConflict    bool
}

func pkgApplyCmd(newSVCFn pkgSVCFn) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pkg",
		Short: "Apply a pkg to create resources",
	}

	var opts pkgApplyOpts
	cmd.Flags().StringVarP(&opts.file, "file", "f", "", "Path to package file")
	cmd.MarkFlagFilename("file", "yaml", "yml", "json")
	cmd.Flags().BoolVar(&opts.forceOnConflict, "force-on-conflict", true, "TTY input, if package will have destructive changes, proceed if set true.")
	cmd.Flags().BoolVarP(&opts.quiet, "quiet", "q", false, "disable output printing")

	cmd.Flags().StringVarP(&opts.orgID, "org-id", "o", "", "The ID of the organization that owns the bucket")
	cmd.MarkFlagRequired("org-id")

	cmd.Flags().BoolVarP(&opts.hasColor, "color", "c", true, "Enable color in output, defaults true")
	cmd.Flags().BoolVar(&opts.hasTableBorders, "table-borders", true, "Enable table borders, defaults true")

	cmd.RunE = pkgApplyRunEFn(newSVCFn, &opts)

	return cmd
}

func pkgApplyRunEFn(newSVCFn pkgSVCFn, opts *pkgApplyOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) (e error) {
		if !opts.hasColor {
			color.NoColor = true
		}

		influxOrgID, err := influxdb.IDFromString(opts.orgID)
		if err != nil {
			return err
		}

		svc, err := newSVCFn(flags.httpClientOpts())
		if err != nil {
			return err
		}

		var (
			pkg   *pkger.Pkg
			isTTY bool
		)
		if stdin, _ := readStdIn(); stdin != nil {
			isTTY = true
			pkg, err = pkgFromStdIn(stdin)
		} else {
			if opts.file == "" {
				return errors.New("a file path is required when not using a TTY input")
			}
			pkg, err = pkgFromFile(opts.file)
		}
		if err != nil {
			return err
		}

		_, diff, err := svc.DryRun(context.Background(), *influxOrgID, pkg)
		if err != nil {
			return err
		}

		if !opts.quiet {
			printPkgDiff(opts.hasColor, opts.hasTableBorders, diff)
		}

		if !isTTY {
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

		if !opts.forceOnConflict && isTTY && diff.HasConflicts() {
			return errors.New("package has conflicts with existing resources and cannot safely apply")
		}

		summary, err := svc.Apply(context.Background(), *influxOrgID, pkg)
		if err != nil {
			return err
		}

		if !opts.quiet {
			printPkgSummary(opts.hasColor, opts.hasTableBorders, summary)
		}

		return nil
	}
}

func pkgNewCmd(newSVCFn pkgSVCFn) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Create a reusable pkg to create resources in a declarative manner",
	}

	var opts pkgNewOpts
	cmd.Flags().StringVarP(&opts.outPath, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().BoolVarP(&opts.quiet, "quiet", "q", false, "skip interactive mode")
	cmd.Flags().StringVarP(&opts.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Version, "version", "v", "", "version for new pkg")

	cmd.RunE = pkgNewRunEFn(newSVCFn, &opts)

	return cmd
}

type pkgNewOpts struct {
	quiet   bool
	outPath string
	orgID   string
	meta    pkger.Metadata
}

func pkgNewRunEFn(newSVCFn pkgSVCFn, opt *pkgNewOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if !opt.quiet {
			ui := &input.UI{
				Writer: os.Stdout,
				Reader: os.Stdin,
			}

			if opt.meta.Name == "" {
				opt.meta.Name = getInput(ui, "pkg name", "")
			}
			if opt.meta.Description == "" {
				opt.meta.Description = getInput(ui, "pkg description", opt.meta.Description)
			}
			if opt.meta.Version == "" {
				opt.meta.Version = getInput(ui, "pkg version", opt.meta.Version)
			}
		}

		pkgSVC, err := newSVCFn(flags.httpClientOpts())
		if err != nil {
			return err
		}

		return writePkg(cmd.OutOrStdout(), pkgSVC, opt.outPath, pkger.CreateWithMetadata(opt.meta))
	}
}

type pkgExportOpts struct {
	outPath      string
	meta         pkger.Metadata
	resourceType string
	buckets      string
	dashboards   string
	labels       string
	variables    string
}

func pkgExportCmd(newSVCFn pkgSVCFn) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export existing resources as a package",
	}
	cmd.AddCommand(pkgExportAllCmd(newSVCFn))

	var opts pkgExportOpts
	cmd.Flags().StringVarP(&opts.outPath, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().StringVarP(&opts.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Version, "version", "v", "", "version for new pkg")
	cmd.Flags().StringVar(&opts.resourceType, "resource-type", "", "The resource type provided will be associated with all IDs via stdin.")
	cmd.Flags().StringVar(&opts.buckets, "buckets", "", "List of bucket ids comma separated")
	cmd.Flags().StringVar(&opts.dashboards, "dashboards", "", "List of dashboard ids comma separated")
	cmd.Flags().StringVar(&opts.labels, "labels", "", "List of label ids comma separated")
	cmd.Flags().StringVar(&opts.variables, "variables", "", "List of variable ids comma separated")

	cmd.RunE = pkgExportRunEFn(newSVCFn, &opts)
	return cmd
}

func pkgExportRunEFn(newSVCFn pkgSVCFn, cmdOpts *pkgExportOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		pkgSVC, err := newSVCFn(flags.httpClientOpts())
		if err != nil {
			return err
		}

		opts := []pkger.CreatePkgSetFn{pkger.CreateWithMetadata(cmdOpts.meta)}

		resTypes := []struct {
			kind   pkger.Kind
			idStrs []string
		}{
			{kind: pkger.KindBucket, idStrs: strings.Split(cmdOpts.buckets, ",")},
			{kind: pkger.KindDashboard, idStrs: strings.Split(cmdOpts.dashboards, ",")},
			{kind: pkger.KindLabel, idStrs: strings.Split(cmdOpts.labels, ",")},
			{kind: pkger.KindVariable, idStrs: strings.Split(cmdOpts.variables, ",")},
		}
		for _, rt := range resTypes {
			newOpt, err := getResourcesToClone(rt.kind, rt.idStrs)
			if err != nil {
				return ierror.Wrap(err, rt.kind.String())
			}
			opts = append(opts, newOpt)
		}

		if cmdOpts.resourceType == "" {
			return writePkg(cmd.OutOrStdout(), pkgSVC, cmdOpts.outPath, opts...)
		}

		kind := pkger.NewKind(cmdOpts.resourceType)
		if err := kind.OK(); err != nil {
			return errors.New("resource type must be one of bucket|dashboard|label|variable; got: " + cmdOpts.resourceType)
		}

		stdinInpt, _ := readStdInLines()
		if len(stdinInpt) > 0 {
			args = stdinInpt
		}

		resTypeOpt, err := getResourcesToClone(kind, args)
		if err != nil {
			return err
		}

		return writePkg(cmd.OutOrStdout(), pkgSVC, cmdOpts.outPath, append(opts, resTypeOpt)...)
	}
}

func getResourcesToClone(kind pkger.Kind, idStrs []string) (pkger.CreatePkgSetFn, error) {
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

func pkgExportAllCmd(newSVCFn pkgSVCFn) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "all",
		Short: "Export all existing resources for an organization as a package",
	}

	var opts pkgNewOpts
	cmd.Flags().StringVarP(&opts.outPath, "file", "f", "", "output file for created pkg; defaults to std out if no file provided; the extension of provided file (.yml/.json) will dictate encoding")
	cmd.Flags().StringVarP(&opts.orgID, "org-id", "o", "", "organization id")
	cmd.Flags().StringVarP(&opts.meta.Name, "name", "n", "", "name for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Description, "description", "d", "", "description for new pkg")
	cmd.Flags().StringVarP(&opts.meta.Version, "version", "v", "", "version for new pkg")

	cmd.RunE = pkgExportAllRunEFn(newSVCFn, &opts)

	return cmd
}

func pkgExportAllRunEFn(newSVCFn pkgSVCFn, opt *pkgNewOpts) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		pkgSVC, err := newSVCFn(flags.httpClientOpts())
		if err != nil {
			return err
		}

		opts := []pkger.CreatePkgSetFn{pkger.CreateWithMetadata(opt.meta)}

		orgID, err := influxdb.IDFromString(opt.orgID)
		if err != nil {
			return err
		}
		opts = append(opts, pkger.CreateWithAllOrgResources(*orgID))

		return writePkg(cmd.OutOrStdout(), pkgSVC, opt.outPath, opts...)
	}
}

func readStdIn() (*os.File, error) {
	info, err := os.Stdin.Stat()
	if err != nil {
		return nil, err
	}
	if (info.Mode() & os.ModeCharDevice) == os.ModeCharDevice {
		return nil, nil
	}
	return os.Stdin, nil
}

func readStdInLines() ([]string, error) {
	r, err := readStdIn()
	if err != nil || r == nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var stdinInput []string
	for _, bb := range bytes.Split(b, []byte("\n")) {
		trimmed := bytes.TrimSpace(bb)
		if len(trimmed) == 0 {
			continue
		}
		stdinInput = append(stdinInput, string(trimmed))
	}
	return stdinInput, nil
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

func writePkg(w io.Writer, pkgSVC pkger.SVC, outPath string, opts ...pkger.CreatePkgSetFn) error {
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

func newPkgerSVC(cliReqOpts httpClientOpts) (pkger.SVC, error) {
	return pkger.NewService(
		pkger.WithBucketSVC(&ihttp.BucketService{
			Addr:               cliReqOpts.addr,
			Token:              cliReqOpts.token,
			InsecureSkipVerify: cliReqOpts.skipVerify,
		}),
		pkger.WithDashboardSVC(&ihttp.DashboardService{
			Addr:               cliReqOpts.addr,
			Token:              cliReqOpts.token,
			InsecureSkipVerify: cliReqOpts.skipVerify,
		}),
		pkger.WithLabelSVC(&ihttp.LabelService{
			Addr:               cliReqOpts.addr,
			Token:              cliReqOpts.token,
			InsecureSkipVerify: cliReqOpts.skipVerify,
		}),
		pkger.WithVariableSVC(&ihttp.VariableService{
			Addr:               cliReqOpts.addr,
			Token:              cliReqOpts.token,
			InsecureSkipVerify: cliReqOpts.skipVerify,
		}),
	), nil
}

func pkgFromStdIn(stdin *os.File) (*pkger.Pkg, error) {
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

func printPkgDiff(hasColor, hasTableBorders bool, diff pkger.Diff) {
	red := color.New(color.FgRed).SprintfFunc()
	green := color.New(color.FgHiGreen, color.Bold).SprintfFunc()

	strDiff := func(isNew bool, old, new string) string {
		if isNew {
			return green(new)
		}
		if old == new {
			return new
		}
		return fmt.Sprintf("%s\n%s", red("%q", old), green("%q", new))
	}

	boolDiff := func(b bool) string {
		bb := strconv.FormatBool(b)
		if b {
			return green(bb)
		}
		return bb
	}

	durDiff := func(isNew bool, oldDur, newDur time.Duration) string {
		o := oldDur.String()
		if oldDur == 0 {
			o = "inf"
		}
		n := newDur.String()
		if newDur == 0 {
			n = "inf"
		}
		if isNew {
			return green(n)
		}
		if oldDur == newDur {
			return n
		}
		return fmt.Sprintf("%s\n%s", red(o), green(n))
	}

	tablePrintFn := tablePrinterGen(hasColor, hasTableBorders)
	if labels := diff.Labels; len(labels) > 0 {
		headers := []string{"New", "ID", "Name", "Color", "Description"}
		tablePrintFn("LABELS", headers, len(labels), func(w *tablewriter.Table) {
			for _, l := range labels {
				w.Append([]string{
					boolDiff(l.IsNew()),
					l.ID.String(),
					l.Name,
					strDiff(l.IsNew(), l.OldColor, l.NewColor),
					strDiff(l.IsNew(), l.OldDesc, l.NewDesc),
				})
			}
		})
	}

	if bkts := diff.Buckets; len(bkts) > 0 {
		headers := []string{"New", "ID", "Name", "Retention Period", "Description"}
		tablePrintFn("BUCKETS", headers, len(bkts), func(w *tablewriter.Table) {
			for _, b := range bkts {
				w.Append([]string{
					boolDiff(b.IsNew()),
					b.ID.String(),
					b.Name,
					durDiff(b.IsNew(), b.OldRetention, b.NewRetention),
					strDiff(b.IsNew(), b.OldDesc, b.NewDesc),
				})
			}
		})
	}

	if dashes := diff.Dashboards; len(dashes) > 0 {
		headers := []string{"New", "Name", "Description", "Num Charts"}
		tablePrintFn("DASHBOARDS", headers, len(dashes), func(w *tablewriter.Table) {
			for _, d := range dashes {
				w.Append([]string{
					boolDiff(true),
					d.Name,
					green(d.Desc),
					green(strconv.Itoa(len(d.Charts))),
				})
			}
		})
	}

	if vars := diff.Variables; len(vars) > 0 {
		headers := []string{"New", "ID", "Name", "Description", "Arg Type", "Arg Values"}
		tablePrintFn("VARIABLES", headers, len(vars), func(w *tablewriter.Table) {
			for _, v := range vars {
				var oldArgType string
				if v.OldArgs != nil {
					oldArgType = v.OldArgs.Type
				}
				var newArgType string
				if v.NewArgs != nil {
					newArgType = v.NewArgs.Type
				}
				w.Append([]string{
					boolDiff(v.IsNew()),
					v.ID.String(),
					v.Name,
					strDiff(v.IsNew(), v.OldDesc, v.NewDesc),
					strDiff(v.IsNew(), oldArgType, newArgType),
					strDiff(v.IsNew(), printVarArgs(v.OldArgs), printVarArgs(v.NewArgs)),
				})
			}
		})
	}

	if len(diff.LabelMappings) > 0 {
		headers := []string{"New", "Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrintFn("LABEL MAPPINGS", headers, len(diff.LabelMappings), func(w *tablewriter.Table) {
			for _, m := range diff.LabelMappings {
				w.Append([]string{
					boolDiff(m.IsNew),
					string(m.ResType),
					m.ResName,
					m.ResID.String(),
					m.LabelName,
					m.LabelID.String(),
				})
			}
		})
	}
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

func printPkgSummary(hasColor, hasTableBorders bool, sum pkger.Summary) {
	tablePrintFn := tablePrinterGen(hasColor, hasTableBorders)
	if labels := sum.Labels; len(labels) > 0 {
		headers := []string{"ID", "Name", "Description", "Color"}
		tablePrintFn("LABELS", headers, len(labels), func(w *tablewriter.Table) {
			for _, l := range labels {
				w.Append([]string{
					l.ID.String(),
					l.Name,
					l.Properties["description"],
					l.Properties["color"],
				})
			}
		})
	}

	if buckets := sum.Buckets; len(buckets) > 0 {
		headers := []string{"ID", "Name", "Retention", "Description"}
		tablePrintFn("BUCKETS", headers, len(buckets), func(w *tablewriter.Table) {
			for _, bucket := range buckets {
				w.Append([]string{
					bucket.ID.String(),
					bucket.Name,
					formatDuration(bucket.RetentionPeriod),
					bucket.Description,
				})
			}
		})
	}

	if dashes := sum.Dashboards; len(dashes) > 0 {
		headers := []string{"ID", "Name", "Description"}
		tablePrintFn("DASHBOARDS", headers, len(dashes), func(w *tablewriter.Table) {
			for _, d := range dashes {
				w.Append([]string{
					d.ID.String(),
					d.Name,
					d.Description,
				})
			}
		})
	}

	if vars := sum.Variables; len(vars) > 0 {
		headers := []string{"ID", "Name", "Description", "Arg Type", "Arg Values"}
		tablePrintFn("VARIABLES", headers, len(vars), func(w *tablewriter.Table) {
			for _, v := range vars {
				args := v.Arguments
				w.Append([]string{
					v.ID.String(),
					v.Name,
					v.Description,
					args.Type,
					printVarArgs(args),
				})
			}
		})
	}

	if mappings := sum.LabelMappings; len(mappings) > 0 {
		headers := []string{"Resource Type", "Resource Name", "Resource ID", "Label Name", "Label ID"}
		tablePrintFn("LABEL MAPPINGS", headers, len(mappings), func(w *tablewriter.Table) {
			for _, m := range mappings {
				w.Append([]string{
					string(m.ResourceType),
					m.ResourceName,
					m.ResourceID.String(),
					m.LabelName,
					m.LabelID.String(),
				})
			}
		})
	}
}

func tablePrinterGen(hasColor, hasTableBorder bool) func(table string, headers []string, count int, appendFn func(w *tablewriter.Table)) {
	return func(table string, headers []string, count int, appendFn func(w *tablewriter.Table)) {
		tablePrinter(table, headers, count, hasColor, hasTableBorder, appendFn)
	}
}

func tablePrinter(table string, headers []string, count int, hasColor, hasTableBorders bool, appendFn func(w *tablewriter.Table)) {
	descrCol := -1
	for i, h := range headers {
		if strings.ToLower(h) == "description" {
			descrCol = i
			break
		}
	}

	w := tablewriter.NewWriter(os.Stdout)
	w.SetBorder(hasTableBorders)
	w.SetRowLine(hasTableBorders)

	var alignments []int
	for range headers {
		alignments = append(alignments, tablewriter.ALIGN_CENTER)
	}
	if descrCol != -1 {
		w.SetColMinWidth(descrCol, 30)
		alignments[descrCol] = tablewriter.ALIGN_LEFT
	}

	color.New(color.FgYellow, color.Bold).Fprintln(os.Stdout, strings.ToUpper(table))
	w.SetHeader(headers)
	w.SetColumnAlignment(alignments)

	appendFn(w)

	footers := make([]string, len(headers))
	footers[len(footers)-2] = "TOTAL"
	footers[len(footers)-1] = strconv.Itoa(count)
	w.SetFooter(footers)
	if hasColor {
		var colors []tablewriter.Colors
		for i := 0; i < len(headers); i++ {
			colors = append(colors, tablewriter.Color(tablewriter.FgHiCyanColor))
		}
		w.SetHeaderColor(colors...)
		colors[len(colors)-2] = tablewriter.Color(tablewriter.FgHiBlueColor)
		colors[len(colors)-1] = tablewriter.Color(tablewriter.FgHiBlueColor)
		w.SetFooterColor(colors...)
	}

	w.Render()
	fmt.Fprintln(os.Stdout)
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "inf"
	}
	return d.String()
}
