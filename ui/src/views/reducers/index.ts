// Libraries
import {produce} from 'immer'

// Types
import {Action} from 'src/views/actions/creators'
import {RemoteDataState, ResourceState} from 'src/types'

export type ViewsState = ResourceState['views']

const initialState = (): ViewsState => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

const viewsReducer = (
  state: ViewsState = initialState(),
  action: Action
): ViewsState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_VIEWS': {
        return
        // const {status} = action.payload
        // if (!action.payload.views) {
        //   return {
        //     ...state,
        //     status,
        //   }
        // }
        // const views = action.payload.views.reduce<ViewsState['views']>(
        //   (acc, view) => ({
        //     ...acc,
        //     [view.id]: {
        //       view,
        //       status: RemoteDataState.Done,
        //     },
        //   }),
        //   {}
        // )
        // return {
        //   status,
        //   views,
        // }
      }
      case 'SET_VIEW': {
        return
        // const {id, view, status} = action.payload
        // return {
        //   ...state,
        //   views: {
        //     ...state.views,
        //     [id]: {view, status},
        //   },
        // }
      }
      case 'RESET_VIEWS': {
        return initialState()
      }
    }
  })

export default viewsReducer
