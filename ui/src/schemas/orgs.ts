// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Organizations */

// Defines the schema for the "organizations" resource
export const orgSchema = new schema.Entity(ResourceType.Orgs)
export const arrayOfOrgs = [orgSchema]
