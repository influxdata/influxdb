// Types
import {Action} from 'src/dashboards/actions/v2/views'
import {RemoteDataState} from 'src/types'
import {View} from 'src/api'

export interface ViewsState {
  [viewID: string]: {
    status: RemoteDataState
    view: View
  }
}

const viewsReducer = (state: ViewsState = {}, action: Action) => {
  switch (action.type) {
    case 'SET_VIEW': {
      const {id, view, status} = action.payload

      return {
        ...state,
        [id]: {view, status},
      }
    }
  }

  return state
}

export default viewsReducer
