export type Action = ActionUpdateScript

export interface ActionUpdateScript {
  type: 'UPDATE_SCRIPT'
  payload: {
    script: string
  }
}

export type UpdateScript = (script: string) => ActionUpdateScript

export const updateScript = (script: string): ActionUpdateScript => {
  return {
    type: 'UPDATE_SCRIPT',
    payload: {script},
  }
}
