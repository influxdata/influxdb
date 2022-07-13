# Pkger: the What and How

Responsibilities

* Translating a declarative [package](#anatomy_of_a_package) file (either JSON | Yaml | Jsonnet) into resources in the platform
* Exporting existing resources in the form of a pkg (either JSON | Yaml)
* Managing the state of a pkg's side effects via a stack

## Anatomy of a package

A package is a collection of resource configurations. 
These resource configurations can be seen in full in the [pkger/testdata](https://github.com/influxdata/influxdb/tree/master/pkger/testdata) directory.
The package itself does not have any state. 
Packages may contain resources that are uniquely identifiable within the platform and some that are not.
If it is desired to use packages in a gitops scenario or in a manner that requires all resources are not duplicated, you will want to explore using a stack.

Properties of package files:

* A package's resources are unique by a combination `kind` and `metadata.name` fields
* A package guarantees that all resources within a package is applied consistently
	* the pkger service manages all state management concerns
* A package does not have any state tracked without a stack
* A package may consist of multiple packages where all uniqueness and state guarantees apply

### Stacks

A stack is a stateful entity for which packages can be applied and managed.
A stack uses a combination of a resource's `kind` and `metadata.name` fields to uniquely identify a resource inside a package and map that to state in the platform.
Via this state, a stack provides the ability to apply a package idempotently. 
Stack's manage the full lifecyle of a package's resources, including creating, updating, and deleting resources.

Packages may contain resources that are not uniquely identifiable within the platform.
For instance, a dashboard resource, does not have any unique identifier within the platform beyond its UUID. 
A stack uses the `metadata.name` field to uniquely identify a resource inside a package and map that to state in the platform.

#### Stacks will manage the following use cases:

We create a stack without any URLs to packages, henceforth identified as S1:

```yaml
# S1 package - initial
kind: Label
metadata:
	name: lucid_einstein
spec:
	name: label_1
---
kind: Bucket
metadta: 
	name: pristine_noir
spec: 
	name: bucket_1
	association:
	 	- kind: Label
		   name: lucid_einstein
---
kind: Dashboard
metadata:
	name: charmed_saratoba
spec:
	name: dash_1
	association:
	 	- kind: Label
		   name: lucid_einstein
```


1. The S1 package (seen above) with all new resources is applied

   * Side effects: all resources are created and a record of all resources (id, res type, etc) is added to the S1 stack record

		<details><summary>Stack Record</summary>
		
		```json
		 {
		   "stack_id": S1_UUID,
		   "createdAt": CreatedAtTimestamp,
		   "updatedAt": CreatedAtTimestamp,
		   "config": {},
		   "resources": [
		     {
		       "kind": "Label",
		       "id": LABEL_UUID,
		       "pkgName": "lucid_einstein"
		     },
		     {
		       "kind": "Bucket",
		       "id": BUCKET_UUID,
		       "pkgName": "pristine_noir",
		       "associations": [
		         {
		           "kind": "Label",
		           "pkgName": "lucid_einstein"
		         }
		       ]
		     },
		     {
		       "kind": "Dashboard",
		       "id": DASHBOARD_UUID,
		       "pkgName": "charmed_saratoba",
		       "associations": [
		         {
		           "kind": "Label",
		           "pkgName": "lucid_einstein"
		         }
		       ]
		     }
		   ]
		 }  
		```
		
		</details>



2. Same S1 package (seen above) is reapplied with no changes from step 1

   * Side effects: nothing, no changes

	   <details><summary>Stack Record</summary>
	
	   ```json
	     {
	       "stack_id": S1_UUID,
	       "createdAt": CreatedAtTimestamp,
	       "updatedAt": CreatedAtTimestamp,
	       "config": {},
	       "resources": [
	         {
	           "kind": "Label",
	           "id": LABEL_UUID,
	           "pkgName": "lucid_einstein"
	         },
	         {
	           "kind": "Bucket",
	           "id": BUCKET_UUID,
	           "pkgName": "pristine_noir",
	           "associations": [
	             {
	               "kind": "Label",
	               "pkgName": "lucid_einstein"
	             }
	           ]
	         },
	         {
	           "kind": "Dashboard",
	           "id": DASHBOARD_UUID,
	           "pkgName": "charmed_saratoba",
	           "associations": [
	             {
	               "kind": "Label",
	               "pkgName": "lucid_einstein"
	             }
	           ]
	         }
	       ]
	     }  
	   ```
	
	   </details>

</br>
</br>





```yaml
# S1 package - updated label name
kind: Label
metadata:
	name: lucid_einstein
spec:
	name: cool label name  #<<<<<< THIS NAME CHANGES
---
kind: Bucket
metadta: 
	name: pristine_noir
# snip - no changes
---
kind: Dashboard
metadata:
	name: charmed_saratoba
# snip - no changes
```

3. The S1 package is applied with an update to the label resource

   * Side effects: platform label (LABEL_UUID) is renamed and  `updatedAt` field in **S1** record is updated 

		<details><summary>Stack Record</summary>
		
		```json
		{
		     "stack_id": S1_UUID,
		     "createdAt": CreatedAtTimestamp,
		     "updatedAt": LABEL_UPDATE_TIMESTAMP,
		     "config": {},
		     "resources": [
		       ... snip, all resoruces are same
		     ]
		}  
		```
		
		</details>



</br>
</br>



```yaml
# S1 package - new reosource added
kind: Label
metadata:
	name: lucid_einstein
# snip - no change
---
kind: Bucket
metadta: 
	name: pristine_noir
# snip - no changes
---
kind: Dashboard
metadata:
	name: charmed_saratoba
# snip - no changes
---
kind: Task  #<<<<<< THIS RESOURCE IS ADDED
metadata:
	name: alcord_mumphries
spec:
	name: task_1
	association:
	 	- kind: Label
		   name: lucid_einstein
```



4. The S1 package is applied with a new resource added

   * Side effects: new task is created and **S1** record is updated

		<details><summary>Stack Record</summary>
		
		```json
		{
		     "stack_id": S1_UUID,
		     "createdAt": CreatedAtTimestamp,
		     "updatedAt": TASK_ADD_TIMESTAMP,
		     "config": {},
		     "resources": [
		       ... snip, all resoruces from before,
		       {
		       	"kind": "Task",
		       	"id": TASK_UUID,
		 				"pkgName": "alcord_mumphries",
		         "associations": [
		           {
		             "kind": "Label",
		             "pkgName": "lucid_einstein"
		           }
		         ]
		       }
		     ]
		}  
		```
		
		</details>


</br>
</br>


```yaml
# S1 package - task resource is removed
kind: Label
metadata:
	name: lucid_einstein
# snip - no change
---
kind: Bucket
metadta: 
	name: pristine_noir
# snip - no changes
---
kind: Dashboard
metadata:
	name: charmed_saratoba
# snip - no changes
```



5. The S1 package is applied with changes that removes an existing resource
   * Side effects: task is deleted from platform and **S1** record is updated

		<details><summary>Stack Record</summary>
		
		```json
		{
		    "stack_id": S1_UUID,
		    "createdAt": CreatedAtTimestamp,
		    "updatedAt": TASK_DELETE_TIMESTAMP,
		    "config": {},
		    "resources": [
		      {
		       "kind": "Label",
		       "id": LABEL_UUID,
		       "pkgName": "lucid_einstein"
		     },
		     {
		       "kind": "Bucket",
		       "id": BUCKET_UUID,
		       "pkgName": "pristine_noir",
		       "associations": [
		         {
		           "kind": "Label",
		           "pkgName": "lucid_einstein"
		         }
		       ]
		     },
		     {
		       "kind": "Dashboard",
		       "id": DASHBOARD_UUID,
		       "pkgName": "charmed_saratoba",
		       "associations": [
		         {
		           "kind": "Label",
		           "pkgName": "lucid_einstein"
		         }
		       ]
		     }
		    ]
		}  
		```
		
		</details>

</br>
</br>


```yaml
# S1 package - label and associations to it are removed
kind: Bucket
metadta: 
	name: pristine_noir
spec: 
	name: bucket_1
---
kind: Dashboard
metadata:
	name: charmed_saratoba
spec:
	name: dash_1
```

6. The S1 package is apllied with label and associations to that label removed 
	* Side effects: label and all label assocations for that label are removed from the platform and **S1** record is updated

		<details><summary>Stack Record</summary>
			
		```json
		{
		    "stack_id": S1_UUID,
		    "createdAt": CreatedAtTimestamp,
		    "updatedAt": Label_DELETE_TIMESTAMP,
		    "config": {},
		    "resources": [
		     {
		       "kind": "Bucket",
		       "id": BUCKET_UUID,
		       "pkgName": "pristine_noir",
		       "associations": []
		     },
		     {
		       "kind": "Dashboard",
		       "id": DASHBOARD_UUID,
		       "pkgName": "charmed_saratoba",
		       "associations": []
		     }
		    ]
		}  
		```
			
		</details>


## From package to platform resources

There are 3 main building blocks that take a package and make the declarative package a reality. The following is a quick overview of the system that manages packages.

1. Parser - parses package
	* informs the user of all validation errors in their package
	* enforces `metadata.name` field uniqueness constraint
2. Service - all the business logic for managing packages
	* handles all state management concerns, including making the entire package applied
	* in case of failure to apply a package, the service guarantees the resoruces are returned to their existing state (if any) before the package was applied
3. HTTP API / CLI - means for user to submit packges to be applied
	* provides the ability to export existing resources as a package, dry run a package, and apply a package
	* all CLI calls go through the HTTP API the same way a user generated request would

	
### Parser internals

The parser converts a package in any of the supported encoding types (JSON|Yaml|Jsonnet), and turns it into a [package model](https://github.com/influxdata/influxdb/blob/7d8bd1e055451d06dd55e6334c43d46261749ed7/pkger/parser.go#L229-L254). The parser handles the following:

* enforces naming uniqueness by `metadata.name`
* split loop refactoring
* returns ALL errors in validation with ability to turn off error checking via validation opts
* trivial to extend to support different encoding types (JSON|Yaml|Jsonnet)

You can explore more the goary details [here](https://github.com/influxdata/influxdb/blob/7d8bd1e055451d06dd55e6334c43d46261749ed7/pkger/parser.go).


### Service internals

The service manages all intracommunication to other services and encapsulates the rules for the package domain. The pkger service depends on every service that we currently sans write and query services. Details of the service dependencies can be found [here](https://github.com/influxdata/influxdb/blob/c926accb42d87c407bcac6bbda753f9a03f9ec95/pkger/service.go#L197-L218):

```go
type Service struct {
	log *zap.Logger

	// internal dependencies
	applyReqLimit int
	idGen         influxdb.IDGenerator
	store         Store
	timeGen       influxdb.TimeGenerator

	// external service dependencies
	bucketSVC   influxdb.BucketService
	checkSVC    influxdb.CheckService
	dashSVC     influxdb.DashboardService
	labelSVC    influxdb.LabelService
	endpointSVC influxdb.NotificationEndpointService
	orgSVC      influxdb.OrganizationService
	ruleSVC     influxdb.NotificationRuleStore
	secretSVC   influxdb.SecretService
	taskSVC     influxdb.TaskService
	teleSVC     influxdb.TelegrafConfigStore
	varSVC      influxdb.VariableService
}
```

The behavior of the servcie includes the following:

1. Dry run a package
2. Apply a package
3. Export a package
4. Initialize a stack

The following sections explore this behavior further.

#### Dry run a package

When a package is submitted for a dry run the service takes the contents of that package and identifies the impact of its application before it is run. This is similar to `terraform plan`. 

> This command is a convenient way to check whether the package matches your expectations without making any changes to real resources. For example, a dry run might be run before committing a change to version control, to create confidence that it will behave as expected.

A dry run requires that the package to be dry run has been parsed and graphed. If it has not, the dry run will do so before it attempts the dry run functionality. When a dry run is executed, the caller will have returned a summary of the package and a detailed diff of the impact of the package were it to be applied.

The package summary is as follows:

```go
type Summary struct {
	Buckets               []SummaryBucket               `json:"buckets"`
	Checks                []SummaryCheck                `json:"checks"`
	Dashboards            []SummaryDashboard            `json:"dashboards"`
	NotificationEndpoints []SummaryNotificationEndpoint `json:"notificationEndpoints"`
	NotificationRules     []SummaryNotificationRule     `json:"notificationRules"`
	Labels                []SummaryLabel                `json:"labels"`
	LabelMappings         []SummaryLabelMapping         `json:"labelMappings"`
	MissingEnvs           []string                      `json:"missingEnvRefs"`
	MissingSecrets        []string                      `json:"missingSecrets"`
	Tasks                 []SummaryTask                 `json:"summaryTask"`
	TelegrafConfigs       []SummaryTelegraf             `json:"telegrafConfigs"`
	Variables             []SummaryVariable             `json:"variables"`
}

type SummaryBucket struct {
	ID          SafeID `json:"id,omitempty"`
	OrgID       SafeID `json:"orgID,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
	// TODO: return retention rules?
	RetentionPeriod   time.Duration  `json:"retentionPeriod"`
	LabelAssociations []SummaryLabel `json:"labelAssociations"`
}

// snip other resources
```

The package diff is as follows:

```go
type Diff struct {
	Buckets               []DiffBucket               `json:"buckets"`
	Checks                []DiffCheck                `json:"checks"`
	Dashboards            []DiffDashboard            `json:"dashboards"`
	Labels                []DiffLabel                `json:"labels"`
	LabelMappings         []DiffLabelMapping         `json:"labelMappings"`
	NotificationEndpoints []DiffNotificationEndpoint `json:"notificationEndpoints"`
	NotificationRules     []DiffNotificationRule     `json:"notificationRules"`
	Tasks                 []DiffTask                 `json:"tasks"`
	Telegrafs             []DiffTelegraf             `json:"telegrafConfigs"`
	Variables             []DiffVariable             `json:"variables"`
}

// DiffBucketValues are the varying values for a bucket.
type DiffBucketValues struct {
	Description    string         `json:"description"`
	RetentionRules retentionRules `json:"retentionRules"`
}

// DiffBucket is a diff of an individual bucket.
type DiffBucket struct {
	ID   SafeID            `json:"id"`
	Name string            `json:"name"`
	New  DiffBucketValues  `json:"new"`
	Old  *DiffBucketValues `json:"old,omitempty"` // using omitempty here to signal there was no prev state with a nil
}

// snip other resources
```

If errors are encountered in the parsing, the dry run will return errors in addition to the package summary and diff.


#### Apply a package

When a package is submitted to be applied, the service takes the contents of that package and identifies the impact of its application before it is run (Dry Run). It then brings the platform to the desired state of the package.

> Apply is used to apply the changes required to reach the desired state of the package

If a package had not been verified by a dry run when applying, it will be done to identify existing state within the platform. This existing state has to be maintained to account for an unexpected event that stops the package from being applied. The side effects created from the application will all be rolled back at this point. If a resource was newly created during the application, it will be removed. For a resource that existed in the platform, it will be returned to its state from before the application took place. The guarantee of state consistency is a best attempt. It is not bullet proof. However, a user can reapply the package and arrive at their desired state therafter. Upon successful application of a package, a summary will be provided to the user.

The service takes advantage of [split loop refactoring](https://refactoring.com/catalog/splitLoop.html) to break the package up by resource. The platform requires certain dependencies be met before a resource is created. For instance, when a label mapping is desired for a new label and bucket, it forces us to guarantee the label exists before creating the label mapping. To accomplish this, labels are always applied first. You can see it in action [here](https://github.com/influxdata/influxdb/blob/c926accb42d87c407bcac6bbda753f9a03f9ec95/pkger/service.go#L1110-L1148):

```go
func (s *Service) Apply(ctx context.Context, orgID, userID influxdb.ID, pkg *Pkg, opts ...ApplyOptFn) (sum Summary, e error) {
	// snipping preceding code

	coordinator := &rollbackCoordinator{sem: make(chan struct{}, s.applyReqLimit)}
	defer coordinator.rollback(s.log, &e, orgID)

	// each grouping here runs for its entirety, then returns an error that
	// is indicative of running all appliers provided. For instance, the labels
	// may have 1 variable fail and one of the buckets fails. The errors aggregate so
	// the caller will be informed of both the failed label variable the failed bucket.
	// the groupings here allow for steps to occur before exiting. The first step is
	// adding the dependencies, resources that are associated by other resources. Then the
	// primary resources. Here we get all the errors associated with them.
	// If those are all good, then we run the secondary(dependent) resources which
	// rely on the primary resources having been created.
	appliers := [][]applier{
		{
			// adds secrets that are referenced it the pkg, this allows user to
			// provide data that does not rest in the pkg.
			s.applySecrets(opt.MissingSecrets),
		},
		{
			// deps for primary resources
			s.applyLabels(pkg.labels()),
		},
		{
			// primary resources, can have relationships to labels
			s.applyVariables(pkg.variables()),
			s.applyBuckets(pkg.buckets()),
			s.applyChecks(pkg.checks()),
			s.applyDashboards(pkg.dashboards()),
			s.applyNotificationEndpoints(pkg.notificationEndpoints()),
			s.applyTasks(pkg.tasks()),
			s.applyTelegrafs(pkg.telegrafs()),
		},
	}

	for _, group := range appliers {
		if err := coordinator.runTilEnd(ctx, orgID, userID, group...); err != nil {
			return Summary{}, internalErr(err)
		}
	}

	// snipping succeeding code

	return pkg.Summary(), nil
}
```

Looking at the above you may have noticed we have groups of appliers. The second group contains the label resources. Each group's individual resources are applied concurrently. The `coordinator.runTilEnd(ctx, orgID, userID, group...)` call takes the group, and fans out all the state changes and processes them concurrently for writes. The label resources are guaranteed to have succeeded before processing the primary resources which can have relationships with the label.

When an issue is encountered that cannot be recovered from, and error is returned, and upon seeing that error we roll back all changes. The `defer coordinator.rollback(s.log, &e, orgID)` line rollsback all resources to their preexisting state. For a more in depth look at that check out [here](https://github.com/influxdata/influxdb/blob/c926accb42d87c407bcac6bbda753f9a03f9ec95/pkger/service.go#L2118-L2167).

#### Exporting existing resources as a package

If a user has put a lot of effort in creating dashboards, notifications, and telegraf configs, we have the ability for them to export that work in the shape of a package :-). This enables them to both share that work within the community or their org, and also source control the changes to dashboards.

Resources can be exported all at once, via the export by organization, by specific resource IDs, a combination of the above, and advanced filtering (i.e. by label name or resource type). You can read up more on the export options [here](https://github.com/influxdata/influxdb/blob/c926accb42d87c407bcac6bbda753f9a03f9ec95/pkger/service.go#L280-L330). 

Each resource that is exported is assigned a uniq `metadata.name` entry. The names are generated and are not strictly required to remain in that shape. If a user decides to use `metadata.name` as the name of the resource, they are free to do so. The only requirement is that within a package every resource type has a unique `metadata.name` per its type. For example each resource kind `metadata.name` field should have be unique amongst all resources of the same kind within a package. 

> Each label should have a unique `metadata.name` field amongst all labels in the package.

#### Initializing a stack

When creating a stack we create an stub stack record that contains all the metadata about that stack. Optionally, a user may set URLs in the stack config. These URLs may be used to apply packages from a remote location (i.e. S3 bucket). A stack looks like the following:

```go
type (
	// Stack is an identifier for stateful application of a package(s). This stack
	// will map created resources from the pkg(s) to existing resources on the
	// platform. This stack is updated only after side effects of applying a pkg.
	// If the pkg is applied, and no changes are had, then the stack is not updated.
	Stack struct {
		ID        influxdb.ID
		OrgID     influxdb.ID
		Name      string
		Desc      string
		URLs      []url.URL
		Resources []StackResource

		influxdb.CRUDLog
	}

	// StackResource is a record for an individual resource side effect genereated from
	// applying a pkg.
	StackResource struct {
		APIVersion string
		ID         influxdb.ID
		Kind       Kind
		Name       string
	}
)
```
