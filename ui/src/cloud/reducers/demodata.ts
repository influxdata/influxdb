//Types
import {Actions} from 'src/cloud/actions/demodata'
import {RemoteDataState, Bucket} from 'src/types'

export interface DemoDataState {
  buckets: Bucket[]
  status: RemoteDataState
}

export const defaultState: DemoDataState = {
  buckets: [],
  status: RemoteDataState.NotStarted,
}

export const demoDataReducer = (
  state = defaultState,
  action: Actions
): DemoDataState => {
  switch (action.type) {
    case 'SET_DEMODATA_STATUS': {
      return {...state, status: action.payload.status}
    }
    case 'SET_DEMODATA_BUCKETS': {
      return {
        ...state,
        status: RemoteDataState.Done,
        buckets: action.payload.buckets,
      }
    }
    default:
      return state
  }
}

export default demoDataReducer
