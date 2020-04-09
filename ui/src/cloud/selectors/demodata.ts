import {get, differenceBy} from 'lodash'
import {AppState, Bucket, DemoBucket} from 'src/types'

export const getNewDemoBuckets = (state: AppState, ownBuckets: Bucket[]) => {
  const demoDataBuckets = get(
    state,
    'cloud.demoData.buckets',
    []
  ) as DemoBucket[]
  return differenceBy(demoDataBuckets, ownBuckets, b => b.id)
}
