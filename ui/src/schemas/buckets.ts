// Libraries
import {schema} from 'normalizr'

// Types
import {ResourceType, GenBucket, Bucket} from 'src/types'
import {labelSchema} from './labels'

// Utils
import {ruleToString} from 'src/utils/formatting'

export const getReadableRetention = (bucket: GenBucket): string => {
  const expire = bucket.retentionRules.find(rule => rule.type === 'expire')

  if (!expire) {
    return 'forever'
  }

  return ruleToString(expire.everySeconds)
}

// Defines the schema for the "buckets" resource
export const bucketSchema = new schema.Entity(
  ResourceType.Buckets,
  {
    labels: [labelSchema],
  },
  {
    processStrategy: (bucket: GenBucket): Omit<Bucket, 'labels'> => {
      return {
        ...bucket,
        readableRetention: getReadableRetention(bucket),
      }
    },
  }
)
export const arrayOfBuckets = [bucketSchema]
