// Libraries
import {schema} from 'normalizr'

// Types
import {
  ResourceType,
  Telegraf,
  Task,
  Label,
  RemoteDataState,
  Variable,
} from 'src/types'

// Utils
import {addLabelDefaults} from 'src/labels/utils'

/* Authorizations */

// Defines the schema for the "authorizations" resource
export const auth = new schema.Entity(ResourceType.Authorizations)
export const arrayOfAuths = [auth]

/* Buckets */

// Defines the schema for the "buckets" resource
export const bucket = new schema.Entity(ResourceType.Buckets)
export const arrayOfBuckets = [bucket]

/* Members */

// Defines the schema for the "members" resource
export const member = new schema.Entity(ResourceType.Members)
export const arrayOfMembers = [member]

/* Organizations */

// Defines the schema for the "organizations" resource
export const org = new schema.Entity(ResourceType.Orgs)
export const arrayOfOrgs = [org]

/* Tasks */
// Defines the schema for the tasks resource
export const task = new schema.Entity(
  ResourceType.Tasks,
  {},
  {
    processStrategy: (task: Task): Task => ({
      ...task,
      labels: addLabels(task),
    }),
  }
)

export const arrayOfTasks = [task]

/* Telegrafs */

// Defines the schema for the "telegrafs" resource
export const telegraf = new schema.Entity(
  ResourceType.Telegrafs,
  {},
  {
    // add buckets to metadata if not present
    processStrategy: (t: Telegraf): Telegraf => {
      if (!t.metadata) {
        return {
          ...t,
          metadata: {
            buckets: [],
          },
        }
      }

      if (!t.metadata.buckets) {
        return {
          ...t,
          metadata: {
            ...t.metadata,
            buckets: [],
          },
        }
      }

      return t
    },
  }
)

export const arrayOfTelegrafs = [telegraf]

/* Scrapers */

// Defines the schema for the "scrapers" resource

export const scraper = new schema.Entity(ResourceType.Scrapers)
export const arrayOfScrapers = [scraper]

/* Variables */

// Defines the schema for the "variables" resource
export const variable = new schema.Entity(
  ResourceType.Variables,
  {},
  {
    processStrategy: (v: Variable): Variable => {
      return {
        ...v,
        labels: addLabels(v),
        status: addStatus(v),
      }
    },
  }
)
export const arrayOfVariables = [variable]

const addStatus = <R extends {status: RemoteDataState}>(resource: R) => {
  return resource.status ? resource.status : RemoteDataState.Done
}

export const addLabels = <R extends {labels?: Label[]}>(
  resource: R
): Label[] => {
  return (resource.labels || []).map(addLabelDefaults)
}
