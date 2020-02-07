// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'

/* Buckets */

// Defines the schema for the "buckets" resource
export const bucketSchema = new schema.Entity(ResourceType.Buckets)
export const arrayOfBuckets = [bucketSchema]
