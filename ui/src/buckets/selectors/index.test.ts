// Funcs
import {isDefaultBucket, MONITORING, sortBucketNames, TASKS} from './index'

describe('Bucket Selector Tests', () => {
  it('should return true when a default bucket is passed', () => {
    expect(isDefaultBucket(MONITORING)).toEqual(true)
    expect(isDefaultBucket(TASKS)).toEqual(true)
  })
  it('should return false when no default bucket is passed', () => {
    const bucketName = 'bucket_list'
    expect(isDefaultBucket('tasks')).toEqual(false)
    expect(isDefaultBucket('monitoring')).toEqual(false)
    expect(isDefaultBucket('RANDOM')).toEqual(false)
  })
  it('should sort the bucket names alphabetically', () => {
    const expectedResult = ['alpha', 'BETA', 'Omega']
    expect(sortBucketNames(['BETA', 'Omega', 'alpha'])).toEqual(expectedResult)
  })
})
