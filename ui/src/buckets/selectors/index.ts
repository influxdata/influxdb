// Types
import {AppState, Bucket, ResourceType} from 'src/types'

// Selectors
import {getAll} from 'src/resources/selectors'

export const SYSTEM = 'system'

export const getBucketByName = (
  state: AppState,
  bucketName: string
): Bucket => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets)
  const bucket = buckets.find(b => b.name === bucketName)
  return bucket
}

export const isSystemBucket = (type: string): boolean => type === SYSTEM

const sortFunc = (a: Bucket, b: Bucket) => {
  const firstBucket = `${a.name}`.toLowerCase()
  const secondBucket = `${b.name}`.toLowerCase()
  if (firstBucket === secondBucket) {
    return 0
  }
  if (firstBucket < secondBucket) {
    return -1
  }
  if (firstBucket > secondBucket) {
    return 1
  }
  return 0
}

export const getSortedBucketNames = (buckets: Bucket[]) => {
  const systemBuckets = []
  const otherBuckets = []
  buckets.forEach(bucket => {
    // separate system buckets from the rest
    if (isSystemBucket(bucket.type)) {
      systemBuckets.push(bucket)
    } else {
      otherBuckets.push(bucket)
    }
  })
  // alphabetize system buckets
  systemBuckets.sort(sortFunc)
  // alphabetize other buckets
  otherBuckets.sort(sortFunc)
  // concat the system buckets to the end of the other buckets and map results
  return otherBuckets.concat(systemBuckets).map(bucket => bucket.name)
}
