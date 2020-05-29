package generate

import (
	"os"
	"strings"
	"text/template"

	"github.com/influxdata/influxdb/v2/pkg/data/gen"
	"github.com/spf13/cobra"
)

var simpleCommand = &cobra.Command{
	Use:   "simple",
	Short: "Generate simple data sets using only CLI flags",
	RunE:  simpleGenerateFE,
}

var simpleFlags struct {
	schemaSpec SchemaSpec
}

func init() {
	simpleCommand.PersistentFlags().SortFlags = false
	simpleCommand.Flags().SortFlags = false
	simpleFlags.schemaSpec.AddFlags(simpleCommand, simpleCommand.Flags())

	Command.AddCommand(simpleCommand)
}

func simpleGenerateFE(_ *cobra.Command, _ []string) error {
	storagePlan, err := flags.storageSpec.Plan()
	if err != nil {
		return err
	}

	storagePlan.PrintPlan(os.Stdout)

	schemaPlan, err := simpleFlags.schemaSpec.Plan(storagePlan)
	if err != nil {
		return err
	}

	schemaPlan.PrintPlan(os.Stdout)
	spec := planToSpec(schemaPlan)

	if err = assignOrgBucket(spec); err != nil {
		return err
	}

	if flags.printOnly {
		return nil
	}

	return exec(storagePlan, spec)
}

var (
	tomlSchema = template.Must(template.New("schema").Parse(`
title = "CLI schema"

[[measurements]]
name = "m0"
sample = 1.0
tags = [
{{- range $i, $e := .Tags }}
	{ name = "tag{{$i}}", source = { type = "sequence", format = "value%s", start = 0, count = {{$e}} } },{{ end }}
]
fields = [
	{ name = "v0", count = {{ .PointsPerSeries }}, source = 1.0 },
]`))
)

func planToSpec(p *SchemaPlan) *gen.Spec {
	var sb strings.Builder
	if err := tomlSchema.Execute(&sb, p); err != nil {
		panic(err)
	}

	spec, err := gen.NewSpecFromToml(sb.String())
	if err != nil {
		panic(err)
	}

	return spec
}
