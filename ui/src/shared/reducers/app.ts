import {combineReducers} from 'redux'

import {AUTOREFRESH_DEFAULT_INTERVAL} from 'src/shared/constants'
import {ActionTypes, Action} from 'src/types/actions/app'
import {TimeZone} from 'src/types'

export interface AppState {
  ephemeral: {
    inPresentationMode: boolean
  }
  persisted: {
    autoRefresh: number
    showTemplateControlBar: boolean
    timeZone: TimeZone
    navTreeExpanded: boolean
  }
}

const initialState: AppState = {
  ephemeral: {
    inPresentationMode: false,
  },
  persisted: {
    autoRefresh: AUTOREFRESH_DEFAULT_INTERVAL,
    showTemplateControlBar: false,
    timeZone: 'Local',
    navTreeExpanded: true,
  },
}

const {
  ephemeral: initialAppEphemeralState,
  persisted: initialAppPersistedState,
} = initialState

const appEphemeralReducer = (
  state = initialAppEphemeralState,
  action: Action
) => {
  switch (action.type) {
    case ActionTypes.EnablePresentationMode: {
      return {
        ...state,
        inPresentationMode: true,
      }
    }

    case ActionTypes.DisablePresentationMode: {
      return {
        ...state,
        inPresentationMode: false,
      }
    }

    default:
      return state
  }
}

const appPersistedReducer = (
  state = initialAppPersistedState,
  action: Action
) => {
  switch (action.type) {
    case ActionTypes.SetAutoRefresh: {
      return {
        ...state,
        autoRefresh: action.payload.milliseconds,
      }
    }

    case ActionTypes.TemplateControlBarVisibilityToggled: {
      const {showTemplateControlBar} = state

      return {...state, showTemplateControlBar: !showTemplateControlBar}
    }

    case ActionTypes.SetTimeZone: {
      const {timeZone} = action.payload

      return {...state, timeZone}
    }

    case ActionTypes.ExpandNavTree: {
      return {
        ...state,
        navTreeExpanded: true,
      }
    }

    case ActionTypes.CollapseNavTree: {
      return {
        ...state,
        navTreeExpanded: false,
      }
    }

    default:
      return state
  }
}

const appReducer = combineReducers<AppState>({
  ephemeral: appEphemeralReducer,
  persisted: appPersistedReducer,
})

export default appReducer
