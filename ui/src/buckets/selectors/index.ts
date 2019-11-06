// Types
import {Bucket} from 'src/types'

export const SYSTEM = 'system'

export const isSystemBucket = (type: string): boolean => type === SYSTEM

export const getSortedBucketNames = (buckets: Bucket[]) =>
  buckets
    .sort((a, b) => {
      const firstBucket = `${a.name}`.toLowerCase()
      const secondBucket = `${b.name}`.toLowerCase()
      if (firstBucket === secondBucket) {
        return 0
      }
      if (isSystemBucket(a.type)) {
        // ensures that the default system types are the last buckets
        return 1
      }
      if (isSystemBucket(b.type)) {
        // ensures that the default system types are the last buckets
        return -1
      }
      if (firstBucket < secondBucket) {
        return -1
      }
      if (firstBucket > secondBucket) {
        return 1
      }
      return 0
    })
    .map(bucket => bucket.name)
