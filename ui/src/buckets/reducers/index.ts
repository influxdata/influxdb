// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState} from 'src/types'
import {Action} from 'src/buckets/actions'
import {Bucket} from '@influxdata/influx'

const initialState = (): BucketsState => ({
  status: RemoteDataState.NotStarted,
  list: [],
})

export interface BucketsState {
  status: RemoteDataState
  list: Bucket[]
}

export const bucketsReducer = (
  state: BucketsState = initialState(),
  action: Action
): BucketsState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_BUCKETS': {
        const {status, list} = action.payload

        draftState.status = status

        if (list) {
          draftState.list = list
        }

        return
      }

      case 'ADD_BUCKET': {
        const {bucket} = action.payload

        draftState.list.push(bucket)

        return
      }

      case 'EDIT_BUCKET': {
        const {bucket} = action.payload
        const {list} = draftState

        draftState.list = list.map(l => {
          if (l.id === bucket.id) {
            return bucket
          }

          return l
        })

        return
      }

      case 'REMOVE_BUCKET': {
        const {id} = action.payload
        const {list} = draftState
        const deleted = list.filter(l => {
          return l.id !== id
        })

        draftState.list = deleted
        return
      }
    }
  })
