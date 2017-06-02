import {combineReducers} from 'redux'

import {AUTOREFRESH_DEFAULT} from 'shared/constants'

const initialState = {
  ephemeral: {
    inPresentationMode: false,
  },
  persisted: {
    autoRefresh: AUTOREFRESH_DEFAULT,
    showTemplateControlBar: false,
  },
}

const {
  ephemeral: initialAppEphemeralState,
  persisted: initialAppPersistedState,
} = initialState

const appEphemeralReducer = (state = initialAppEphemeralState, action) => {
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

const appPersistedReducer = (state = initialAppPersistedState, action) => {
  switch (action.type) {
    case 'SET_AUTOREFRESH': {
      return {
        ...state,
        autoRefresh: action.payload.milliseconds,
      }
    }

    case 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED': {
      const {showTemplateControlBar} = state

      return {...state, showTemplateControlBar: !showTemplateControlBar}
    }

    default:
      return state
  }
}

const appReducer = combineReducers({
  ephemeral: appEphemeralReducer,
  persisted: appPersistedReducer,
})

export default appReducer
