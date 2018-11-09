// Types
import {Action} from 'src/dashboards/actions/v2/views'
import {RemoteDataState} from 'src/types'
import {View} from 'src/types/v2'

export interface ViewsState {
  views: {
    [viewID: string]: {
      status: RemoteDataState
      view: View
    }
  }
}

const INITIAL_STATE = {
  // TODO: Flatten me
  views: {},
}

const viewsReducer = (state: ViewsState = INITIAL_STATE, action: Action) => {
  switch (action.type) {
    case 'SET_VIEW': {
      const {id, view, status} = action.payload

      return {
        ...state,
        views: {
          ...state.views,
          [id]: {view, status},
        },
      }
    }
  }

  return state
}

export default viewsReducer
