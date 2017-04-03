import {combineReducers} from 'redux'

import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

const initialState = {
  ephemeral: {
    inPresentationMode: false,
  },
  persisted: {
    autoRefresh: AUTOREFRESH_DEFAULT,
  },
}

const {
  ephemeral: initialEphemeralState,
  persisted: initialPersistedState,
} = initialState

const ephemeralReducer = (state = initialEphemeralState, action) => {
  switch (action.type) {
    case 'ENABLE_PRESENTATION_MODE': {
      return {
        ...state,
        inPresentationMode: true,
      }
    }

    case 'DISABLE_PRESENTATION_MODE': {
      return {
        ...state,
        inPresentationMode: false,
      }
    }

    default:
      return state
  }
}

const persistedReducer = (state = initialPersistedState, action) => {
  switch (action.type) {
    case 'SET_AUTOREFRESH': {
      return {
        ...state,
        autoRefresh: action.payload.milliseconds,
      }
    }

    default:
      return state
  }
}

export default combineReducers({
  ephemeral: ephemeralReducer,
  persisted: persistedReducer,
})
