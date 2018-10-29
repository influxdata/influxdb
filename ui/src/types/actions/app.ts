import {Dispatch} from 'redux'

export enum ActionTypes {
  EnablePresentationMode = 'ENABLE_PRESENTATION_MODE',
  DisablePresentationMode = 'DISABLE_PRESENTATION_MODE',
  SetAutoRefresh = 'SET_AUTOREFRESH',
  TemplateControlBarVisibilityToggled = 'TemplateControlBarVisibilityToggledAction',
  Noop = 'NOOP',
}

export type Action =
  | EnablePresentationModeAction
  | DisablePresentationModeAction
  | SetAutoRefreshAction
  | TemplateControlBarVisibilityToggledAction

export type EnablePresentationModeActionCreator = () => EnablePresentationModeAction

export interface EnablePresentationModeAction {
  type: ActionTypes.EnablePresentationMode
}

export interface DisablePresentationModeAction {
  type: ActionTypes.DisablePresentationMode
}

export type DelayEnablePresentationModeDispatcher = () => DelayEnablePresentationModeThunk

export type DelayEnablePresentationModeThunk = (
  dispatch: Dispatch<EnablePresentationModeAction>
) => Promise<NodeJS.Timer>

export type SetAutoRefreshActionCreator = (
  milliseconds: number
) => SetAutoRefreshAction

export interface SetAutoRefreshAction {
  type: ActionTypes.SetAutoRefresh
  payload: {
    milliseconds: number
  }
}

export type TemplateControlBarVisibilityToggledActionCreator = () => TemplateControlBarVisibilityToggledAction

export interface TemplateControlBarVisibilityToggledAction {
  type: ActionTypes.TemplateControlBarVisibilityToggled
}
