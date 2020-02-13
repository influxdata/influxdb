// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType} from 'src/types'
import {labelSchema} from './labels'
/* Buckets */

// Defines the schema for the "buckets" resource
export const bucketSchema = new schema.Entity(ResourceType.Buckets, {
  labels: [labelSchema],
})
export const arrayOfBuckets = [bucketSchema]
