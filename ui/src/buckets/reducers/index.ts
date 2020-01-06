// Libraries
import {produce} from 'immer'

// Types
import {Bucket, RemoteDataState, ResourceState, ResourceType} from 'src/types'
import {
  ADD_BUCKET,
  SET_BUCKETS,
  Action,
  EDIT_BUCKET,
  REMOVE_BUCKET,
} from 'src/buckets/actions/creators'

// Utils
import {
  setResource,
  addResource,
  removeResource,
  editResource,
} from 'src/resources/reducers/helpers'

const {Buckets} = ResourceType
type BucketsState = ResourceState['buckets']

const initialState = (): BucketsState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

export const bucketsReducer = (
  state: BucketsState = initialState(),
  action: Action
): BucketsState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_BUCKETS: {
        setResource<Bucket>(draftState, action, Buckets)

        return
      }

      case ADD_BUCKET: {
        addResource<Bucket>(draftState, action, Buckets)

        return
      }

      case EDIT_BUCKET: {
        editResource<Bucket>(draftState, action, Buckets)

        return
      }

      case REMOVE_BUCKET: {
        removeResource<Bucket>(draftState, action)

        return
      }
    }
  })
