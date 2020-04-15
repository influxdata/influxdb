import {
  Action,
  SET_CURRENT_DASHBOARD,
} from 'src/shared/actions/currentDashboard'

export interface CurrentDashboardState {
  id: string
}

export const initialState: CurrentDashboardState = {
  id: '',
}

const reducer = (
  state: CurrentDashboardState = initialState,
  action: Action
): CurrentDashboardState => {
  switch (action.type) {
    case SET_CURRENT_DASHBOARD:
      state.id = action.id
      return {...state}
    default:
      return state
  }
}

export default reducer
