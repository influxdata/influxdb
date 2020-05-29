import {startsWith} from 'lodash'

export const MIN_RETENTION_SECONDS = 3600

export const isSystemBucket = (bucketName: string): boolean => {
  return startsWith(bucketName, '_')
}
