import {PRESENTATION_MODE_ANIMATION_DELAY} from '../constants'

import {
  notify,
  PublishNotificationAction,
} from 'src/shared/actions/notifications'
import {presentationMode} from 'src/shared/copy/notifications'

import {Dispatch} from 'redux'
import {TimeZone, Theme, NavBarState, NotebookMiniMapState} from 'src/types'

export enum ActionTypes {
  EnablePresentationMode = 'ENABLE_PRESENTATION_MODE',
  DisablePresentationMode = 'DISABLE_PRESENTATION_MODE',
  EnableVEOMode = 'ENABLE_VEO_MODE',
  DisableVEOMode = 'DISABLE_VEO_MODE',
  SetNavBarState = 'SET_NAV_BAR_STATE',
  SetNotebookMiniMapState = 'SET_NOTEBOOK_MINI_MAP_STATE',
  SetAutoRefresh = 'SET_AUTOREFRESH',
  SetTimeZone = 'SET_APP_TIME_ZONE',
  TemplateControlBarVisibilityToggled = 'TemplateControlBarVisibilityToggledAction',
  Noop = 'NOOP',
}

export type Action =
  | ReturnType<typeof enablePresentationMode>
  | ReturnType<typeof disablePresentationMode>
  | ReturnType<typeof enableVEOMode>
  | ReturnType<typeof disableVEOMode>
  | ReturnType<typeof setNavBarState>
  | ReturnType<typeof setNotebookMiniMapState>
  | ReturnType<typeof setAutoRefresh>
  | ReturnType<typeof setTimeZone>
  | ReturnType<typeof setTheme>

// ephemeral state action creators

export const enablePresentationMode = () =>
  ({
    type: ActionTypes.EnablePresentationMode,
  } as const)

export const disablePresentationMode = () =>
  ({
    type: ActionTypes.DisablePresentationMode,
  } as const)

export const enableVEOMode = () =>
  ({
    type: ActionTypes.EnableVEOMode,
  } as const)

export const disableVEOMode = () =>
  ({
    type: ActionTypes.DisableVEOMode,
  } as const)

export const delayEnablePresentationMode = () => (
  dispatch: Dispatch<
    ReturnType<typeof enablePresentationMode> | PublishNotificationAction
  >
): NodeJS.Timer =>
  setTimeout(() => {
    dispatch(enablePresentationMode())
    dispatch(notify(presentationMode()))
  }, PRESENTATION_MODE_ANIMATION_DELAY)

// persistent state action creators

export const setTheme = (theme: Theme) => ({type: 'SET_THEME', theme} as const)

export const setNavBarState = (navBarState: NavBarState) =>
  ({
    type: ActionTypes.SetNavBarState,
    navBarState,
  } as const)

export const setNotebookMiniMapState = (
  notebookMiniMapState: NotebookMiniMapState
) =>
  ({
    type: ActionTypes.SetNotebookMiniMapState,
    notebookMiniMapState,
  } as const)

export const setAutoRefresh = (milliseconds: number) =>
  ({
    type: ActionTypes.SetAutoRefresh,
    payload: {
      milliseconds,
    },
  } as const)

export const setTimeZone = (timeZone: TimeZone) =>
  ({
    type: ActionTypes.SetTimeZone,
    payload: {timeZone},
  } as const)
