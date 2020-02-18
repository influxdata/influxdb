import {Action, SET_DASHBOARD} from 'src/shared/actions/currentDashboard'

export interface CurrentDashboardState {
  dashboard: string
}

export const initialState: CurrentDashboardState = {
  dashboard: '',
}

const reducer = (
  state: CurrentDashboardState = initialState,
  action: Action
): CurrentDashboardState => {
  switch (action.type) {
    case SET_DASHBOARD:
      state.dashboard = action.id
      return {...state}
    default:
      return state
  }
}

export default reducer
