// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Authorizations */

// Defines the schema for the "auth" resource
export const auth = new schema.Entity(ResourceType.Authorizations)
export const arrayOfAuths = [auth]

/* Buckets */

// Defines the schema for the "bucket" resource
export const bucket = new schema.Entity(ResourceType.Buckets)
export const arrayOfBuckets = [bucket]

/* Members */

// Defines the schema for the "member" resource
export const member = new schema.Entity(ResourceType.Members)
export const arrayOfMembers = [member]

/* Organizations */

// Defines the schema for the "member" resource
export const org = new schema.Entity(ResourceType.Orgs)
export const arrayOfOrgs = [org]
