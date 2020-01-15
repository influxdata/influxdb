// Libraries
import {produce} from 'immer'

// Types
import {ResourceState, RemoteDataState} from 'src/types'

type CellsState = ResourceState['cells']

const initialState = () => ({
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
})

export const cellsReducer = (state: CellsState = initialState(), action) =>
  produce(state, draftState => {
    switch (action.type) {
      case 'MY_FIRST_ACTION': {
        return draftState
      }
    }
  })
