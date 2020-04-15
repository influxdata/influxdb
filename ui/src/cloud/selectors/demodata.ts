import {get, differenceBy, sortBy} from 'lodash'
import {AppState, Bucket, DemoBucket} from 'src/types'

export const getNewDemoBuckets = (state: AppState, ownBuckets: Bucket[]) => {
  const demoDataBuckets = get(
    state,
    'cloud.demoData.buckets',
    []
  ) as DemoBucket[]

  const newDemoDataBuckets = differenceBy(
    demoDataBuckets,
    ownBuckets,
    b => b.id
  )

  return sortBy(newDemoDataBuckets, d => {
    return d.name.toLocaleLowerCase()
  })
}
