// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Organizations */

// Defines the schema for the "organizations" resource
export const org = new schema.Entity(ResourceType.Orgs)
export const arrayOfOrgs = [org]

/* Scrapers */

// Defines the schema for the "scrapers" resource

export const scraper = new schema.Entity(ResourceType.Scrapers)
export const arrayOfScrapers = [scraper]

export {authSchema, arrayOfAuths} from './authorizations'
export {bucketSchema, arrayOfBuckets} from './buckets'
export {checkSchema, arrayOfChecks} from './checks'
export {
  dashboardSchema,
  arrayOfDashboards,
  cellSchema,
  arrayOfCells,
  viewSchema,
  arrayOfViews,
} from './dashboards'
export {endpointSchema, arrayOfEndpoints} from './endpoints'
export {labelSchema, arrayOfLabels} from './labels'
export {memberSchema, arrayOfMembers} from './members'
export {ruleSchema, arrayOfRules} from './rules'
export {taskSchema, arrayOfTasks} from './tasks'
export {telegrafSchema, arrayOfTelegrafs} from './telegrafs'
export {templateSchema, arrayOfTemplates} from './templates'
export {variableSchema, arrayOfVariables} from './variables'
