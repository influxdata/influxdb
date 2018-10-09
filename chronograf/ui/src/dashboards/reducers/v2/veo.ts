// Actions
import {Action} from 'src/dashboards/actions/v2/veo'

export interface VEOState {
  viewName: string
}

export const initialState: VEOState = {
  viewName: '',
}

const veoReducer = (state = initialState, action: Action): VEOState => {
  switch (action.type) {
    case 'SET_VIEW_NAME': {
      const {viewName} = action.payload

      return {...state, viewName}
    }
  }

  return state
}

export default veoReducer
