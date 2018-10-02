export type Action = ActionCompleteSetup

export enum ActionTypes {
  CompleteSetup = 'COMPLETE_SETUP',
}

export interface ActionCompleteSetup {
  type: ActionTypes.CompleteSetup
  payload: {isSetupComplete: true}
}

export type CompleteSetup = () => ActionCompleteSetup

export const completeSetup = (): ActionCompleteSetup => {
  return {
    type: ActionTypes.CompleteSetup,
    payload: {isSetupComplete: true},
  }
}
