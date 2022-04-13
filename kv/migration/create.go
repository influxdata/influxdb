package migration

import (
	"bytes"
	"fmt"
	"go/format"
	"html/template"
	"os"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const newMigrationFmt = `package all

var %s = &Migration{}
`

// CreateNewMigration persists a new migration file in the appropriate location
// and updates the appropriate all.go list of migrations
func CreateNewMigration(existing []Spec, name string) error {
	camelName := strings.Replace(cases.Title(language.Und).String(name), " ", "", -1)

	newMigrationNumber := len(existing) + 1

	newMigrationVariable := fmt.Sprintf("Migration%04d_%s", newMigrationNumber, camelName)

	newMigrationFile := fmt.Sprintf("./kv/migration/all/%04d_%s.go", newMigrationNumber, strings.Replace(name, " ", "-", -1))

	fmt.Println("Creating new migration:", newMigrationFile)

	if err := os.WriteFile(newMigrationFile, []byte(fmt.Sprintf(newMigrationFmt, newMigrationVariable)), 0644); err != nil {
		return err
	}

	fmt.Println("Inserting migration into ./kv/migration/all/all.go")

	tmplData, err := os.ReadFile("./kv/migration/all/all.go")
	if err != nil {
		return err
	}

	type Context struct {
		Name     string
		Variable string
	}

	tmpl := template.Must(
		template.
			New("migrations").
			Funcs(template.FuncMap{"do_not_edit": func(c Context) string {
				return fmt.Sprintf("%s\n%s,\n// {{ do_not_edit . }}", c.Name, c.Variable)
			}}).
			Parse(string(tmplData)),
	)

	buf := new(bytes.Buffer)

	if err := tmpl.Execute(buf, Context{
		Name:     name,
		Variable: newMigrationVariable,
	}); err != nil {
		return err
	}

	src, err := format.Source(buf.Bytes())
	if err != nil {
		return err
	}

	if err := os.WriteFile("./kv/migration/all/all.go", src, 0644); err != nil {
		return err
	}

	return nil
}
