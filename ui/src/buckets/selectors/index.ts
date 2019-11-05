// Types
import {Bucket} from 'src/types'

export const MONITORING = '_monitoring'
export const TASKS = '_tasks'

export const isDefaultBucket = (name: string): boolean =>
  name === MONITORING || name === TASKS

export const sortBucketNames = (buckets: Bucket[] ) =>
  buckets
    .map(bucket => bucket.name)
    .sort((a, b) => {
      if (isDefaultBucket(a)) {
        // ensures that the default _monitoring && _tasks are the last buckets
        return 1
      }
      if (`${a}`.toLowerCase() < `${b}`.toLowerCase()) {
        return -1
      }
      if (`${a}`.toLowerCase() > `${b}`.toLowerCase()) {
        return 1
      }
      return 0
    })
