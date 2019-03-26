// Types
import {Action} from 'src/dashboards/actions/views'
import {RemoteDataState} from 'src/types'
import {View} from 'src/types'

export interface ViewsState {
  status: RemoteDataState
  views: {
    [viewID: string]: {
      status: RemoteDataState
      view: View
    }
  }
}

const initialState = () => ({
  status: RemoteDataState.NotStarted,
  views: {},
})

const viewsReducer = (
  state: ViewsState = initialState(),
  action: Action
): ViewsState => {
  switch (action.type) {
    case 'SET_VIEWS': {
      const {status} = action.payload

      if (!action.payload.views) {
        return {
          ...state,
          status,
        }
      }

      const views = action.payload.views.reduce<ViewsState['views']>(
        (acc, view) => ({
          ...acc,
          [view.id]: {
            view,
            status: RemoteDataState.Done,
          },
        }),
        {}
      )

      return {
        status,
        views,
      }
    }

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
