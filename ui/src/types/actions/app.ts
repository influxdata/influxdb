import {Dispatch} from 'redux'

export type EnablePresentationModeActionCreator = () => EnablePresentationModeAction

export interface EnablePresentationModeAction {
  type: 'ENABLE_PRESENTATION_MODE'
}

export interface DisablePresentationModeAction {
  type: 'DISABLE_PRESENTATION_MODE'
}

export type DelayEnablePresentationModeDispatcher = () => DelayEnablePresentationModeThunk

export type DelayEnablePresentationModeThunk = (
  dispatch: Dispatch<EnablePresentationModeAction>
) => Promise<NodeJS.Timer>

export type SetAutoRefreshActionCreator = (
  milliseconds: number
) => SetAutoRefreshAction

export interface SetAutoRefreshAction {
  type: 'SET_AUTOREFRESH'
  payload: {
    milliseconds: number
  }
}

export type TemplateControlBarVisibilityToggledActionCreator = () => TemplateControlBarVisibilityToggledAction

export interface TemplateControlBarVisibilityToggledAction {
  type: 'TEMPLATE_CONTROL_BAR_VISIBILITY_TOGGLED'
}

export interface NoopAction {
  type: 'NOOP'
  payload: object
}
