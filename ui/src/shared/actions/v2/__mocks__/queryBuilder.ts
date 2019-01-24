import {
  setBuilderBuckets,
  selectBucket,
  Action,
} from 'src/shared/actions/v2/queryBuilder'

// Types
import {Dispatch} from 'redux-thunk'

export const loadBuckets = () => (dispatch: Dispatch<Action>) => {
  const buckets = ['defbuk', 'telegraf']
  dispatch(setBuilderBuckets(buckets))
  dispatch(selectBucket(buckets[0], true))
}
