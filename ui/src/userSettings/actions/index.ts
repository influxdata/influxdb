export type ActionTypes = ToggleShowVariablesControlsAction

interface ToggleShowVariablesControlsAction {
  type: 'TOGGLE_SHOW_VARIABLES_CONTROLS'
}

export const toggleShowVariablesControls = (): ToggleShowVariablesControlsAction => ({
  type: 'TOGGLE_SHOW_VARIABLES_CONTROLS',
})
