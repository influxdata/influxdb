import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

const initialState = {
  autoRefresh: AUTOREFRESH_DEFAULT,
}

const appConfig = (state = initialState, action) => {
  switch (action.type) {
    case 'SET_AUTOREFRESH': {
      return {
        ...state,
        autoRefresh: action.payload.milliseconds,
      }
    }
  }

  return state
}

export default appConfig
