import {combineReducers} from 'redux'

// Types
import {ActionTypes, Action} from 'src/shared/actions/app'
import {AUTOREFRESH_DEFAULT_INTERVAL} from 'src/shared/constants'
import {TimeZone, NavBarState, Theme, NotebookMiniMapState} from 'src/types'

export interface AppState {
  ephemeral: {
    inPresentationMode: boolean
    inVEOMode: boolean
    hasUpdatedTimeRangeInVEO: boolean
  }
  persisted: {
    autoRefresh: number
    showTemplateControlBar: boolean
    timeZone: TimeZone
    navBarState: NavBarState
    theme: Theme
    notebookMiniMapState: NotebookMiniMapState
  }
}

const initialState: AppState = {
  ephemeral: {
    inPresentationMode: false,
    inVEOMode: false,
    hasUpdatedTimeRangeInVEO: false,
  },
  persisted: {
    theme: 'dark',
    autoRefresh: AUTOREFRESH_DEFAULT_INTERVAL,
    showTemplateControlBar: false,
    timeZone: 'Local',
    navBarState: 'collapsed',
    notebookMiniMapState: 'collapsed',
  },
}

const {
  ephemeral: initialAppEphemeralState,
  persisted: initialAppPersistedState,
} = initialState

const appEphemeralReducer = (
  state = initialAppEphemeralState,
  action: Action
): AppState['ephemeral'] => {
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

    case ActionTypes.EnableVEOMode: {
      return {
        ...state,
        inVEOMode: true,
      }
    }

    case ActionTypes.DisableVEOMode: {
      return {
        ...state,
        inVEOMode: false,
      }
    }

    case ActionTypes.EnableUpdatedTimeRangeInVEO: {
      return {
        ...state,
        hasUpdatedTimeRangeInVEO: true,
      }
    }

    case ActionTypes.DisableUpdatedTimeRangeInVEO: {
      return {
        ...state,
        hasUpdatedTimeRangeInVEO: false,
      }
    }

    default:
      return state
  }
}

const appPersistedReducer = (
  state = initialAppPersistedState,
  action: Action
): AppState['persisted'] => {
  switch (action.type) {
    case 'SET_THEME': {
      return {...state, theme: action.theme}
    }

    case ActionTypes.SetAutoRefresh: {
      return {
        ...state,
        autoRefresh: action.payload.milliseconds,
      }
    }

    case ActionTypes.SetTimeZone: {
      const {timeZone} = action.payload

      return {...state, timeZone}
    }

    case ActionTypes.SetNotebookMiniMapState: {
      const notebookMiniMapState = action.notebookMiniMapState

      return {...state, notebookMiniMapState}
    }

    case 'SET_NAV_BAR_STATE': {
      const navBarState = action.navBarState
      return {
        ...state,
        navBarState,
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
