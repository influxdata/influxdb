// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Authorizations */

// Defines the schema for the "authorizations" resource
export const authSchema = new schema.Entity(ResourceType.Authorizations)
export const arrayOfAuths = [authSchema]
