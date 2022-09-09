/*
Package pkger implements a means to create and consume reusable
templates for what will eventually come to support all influxdb
resources.

The parser supports JSON, Jsonnet, and YAML encodings as well as a number
of different ways to read the file/reader/string as it may. While the parser
supports Jsonnet, due to issues in the go-jsonnet implementation, only trusted
input should be given to the parser and therefore the EnableJsonnet() option
must be used with Parse() to enable Jsonnet support.

As an example, you can use the following to parse and validate a YAML
file and see a summary of its contents:

	newTemplate, err := Parse(EncodingYAML, FromFile(PATH_TO_FILE))
	if err != nil {
		panic(err) // handle error as you see fit
	}
	sum := newTemplate.Summary()
	fmt.Println(sum) // do something with the summary

The parser will validate all contents of the template and provide any
and all fields/entries that failed validation.

If you wish to use the Template type in your transport layer and let the
transport layer manage the decoding, then you can run the following
to validate the template after the raw decoding is done:

	if err := template.Validate(); err != nil {
		panic(err) // handle error as you see fit
	}

If a validation error is encountered during the validation or parsing then
the error returned will be of type *parseErr. The parseErr provides a rich
set of validations failures. There can be numerous failures in a template
and we did our best to inform the caller about them all in a single run.

If you want to see the effects of a template before applying it to the
organization's influxdb platform, you have the flexibility to dry run the
template and see the outcome of what would happen after it were to be applied.
You may use the following to dry run a template within your organization:

	svc := NewService(serviceOpts...)
	summary, diff, err := svc.DryRun(ctx, orgID, userID, ApplyWithTemplate(template))
	if err != nil {
		panic(err) // handle error as you see fit
	}
	// explore the summary and diff

The diff provided here is a diff of the existing state of the platform for
your organization and the concluding the state after the application of a
template. All buckets, labels, and variables, when given a name that already
exists, will not create a new resource, but rather, will edit the existing
resource. If this is not a desired result, then rename your bucket to something
else to avoid the imposed changes applying this template would incur. The summary
provided is a summary of the template itself. If a resource exists all IDs will
be populated for them, if they do not, then they will be zero values. Any zero
value ID is safe to assume is not populated. All influxdb.ID's must be non zero
to be in existence.

If you would like to apply a template you may use the service to do so. The
following will apply the template in full to the provided organization.

	svc := NewService(serviceOpts...)
	summary, err := svc.Apply(ctx, orgID, userID, ApplyWithTemplate(template))
	if err != nil {
		panic(err) // handle error as you see fit
	}
	// explore the summary

The summary will be populated with valid IDs that were created during the
application of the template. If an error is encountered during the application
of a template, then all changes that had occurred will be rolled back. However, as
a warning for buckets, changes may have incurred destructive changes. The changes
are not applied inside a large transaction, for numerous reasons, but it is
something to be considered. If you have dry run the template before it is to be
applied, then the changes should have been made known to you. If not, then there is
potential loss of data if the changes to a bucket resulted in the retention period
being shortened in the template.

If you would like to export existing resources into the form of a template, then you
have the ability to do so using the following:

	resourcesToClone := []ResourceToClone{
		{
			Kind: KindBucket,
			ID:   Existing_BUCKET_ID,
			Name: "new bucket name"
		},
		{
			Kind: KindDashboard,
			ID:   Existing_Dashboard_ID,
		},
		{
			Kind: KindLabel,
			ID:   Existing_Label_ID,
		},
		{
			Kind: KindVarible,
			ID:   Existing_Var_ID,
		},
	}

	svc := NewService(serviceOpts...)
	newTemplate, err := svc.Export(ctx, ExportWithExistingResources(resourcesToClone...))
	if err != nil {
		panic(err) // handle error as you see fit
	}
	// explore newly created and validated template

Things to note about the behavior of exporting existing resources. All label
associations with existing resources will be included in the new template.
However, the variables that are used within a dashboard query will not be added
automatically to the template. Variables will need to be passed in alongside
the dashboard to be added to the template.
*/
package pkger
