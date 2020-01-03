// Libraries
import {produce} from 'immer'
import {get} from 'lodash'

// Types
import {RemoteDataState, ResourceState} from 'src/types'
import {
  ADD_BUCKET,
  SET_BUCKETS,
  Action,
  EDIT_BUCKET,
  REMOVE_BUCKET,
} from 'src/buckets/actions/creators'

type BucketsState = ResourceState['buckets']

const initialState = (): BucketsState => ({
  status: RemoteDataState.NotStarted,
  byID: null,
  allIDs: [],
})

export const bucketsReducer = (
  state: BucketsState = initialState(),
  action: Action
): BucketsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_BUCKETS: {
        const {status, schema} = action

        draftState.status = status

        if (get(schema, 'entities')) {
          draftState.byID = schema.entities.buckets
          draftState.allIDs = schema.result
        }

        return
      }

      case ADD_BUCKET: {
        const {result, entities} = action.schema
        const bucket = entities.buckets[result]

        draftState.byID[result] = bucket
        draftState.allIDs.push(result)

        return
      }

      case EDIT_BUCKET: {
        const {entities, result} = action.schema
        const bucket = entities.buckets[result]
        draftState.byID[result] = bucket

        return
      }

      case REMOVE_BUCKET: {
        const {id} = action

        delete draftState.byID[id]
        draftState.allIDs = draftState.allIDs.filter(uuid => uuid !== id)

        return
      }
    }
  })
