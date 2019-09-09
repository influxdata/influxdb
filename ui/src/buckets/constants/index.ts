import {Bucket} from 'src/types'
import {startsWith} from 'lodash'

export const MIN_RETENTION_SECONDS = 3600

export const isSystemBucket = (bucket: Bucket): boolean => {
  return startsWith(bucket.name, '_')
}
