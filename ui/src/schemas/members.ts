// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Members */

// Defines the schema for the "members" resource
export const memberSchema = new schema.Entity(ResourceType.Members)
export const arrayOfMembers = [memberSchema]
