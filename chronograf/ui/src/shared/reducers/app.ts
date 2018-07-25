import {combineReducers} from 'redux'

import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'
import {ActionTypes, Action} from 'src/types/actions/app'

interface State {
  ephemeral: {
    inPresentationMode: boolean
  }
  persisted: {
    autoRefresh: number
    showTemplateControlBar: boolean
  }
}

const initialState: State = {
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

    default:
      return state
  }
}

const appReducer = combineReducers<State>({
  ephemeral: appEphemeralReducer,
  persisted: appPersistedReducer,
})

export default appReducer
