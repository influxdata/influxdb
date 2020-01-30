// Types
import {NotificationRuleDraft, Label, RemoteDataState} from 'src/types'

export type Action =
  | ReturnType<typeof setRules>
  | ReturnType<typeof setRule>
  | ReturnType<typeof setCurrentRule>
  | ReturnType<typeof removeRule>
  | ReturnType<typeof addLabelToRule>
  | ReturnType<typeof removeLabelFromRule>

export const SET_RULES = 'SET_RULES'
export const SET_RULE = 'SET_RULE'
export const SET_CURRENT_RULE = 'SET_CURRENT_RULE'
export const REMOVE_RULE = 'REMOVE_RULE'
export const ADD_LABEL_TO_RULE = 'ADD_LABEL_TO_RULE'
export const REMOVE_LABEL_FROM_RULE = 'REMOVE_LABEL_FROM_RULE'

export const setRules = (
  status: RemoteDataState,
  rules?: NotificationRuleDraft[]
) =>
  ({
    type: SET_RULES,
    payload: {status, rules},
  } as const)

export const setRule = (rule: NotificationRuleDraft) =>
  ({
    type: SET_RULE,
    payload: {rule},
  } as const)

export const setCurrentRule = (
  status: RemoteDataState,
  rule?: NotificationRuleDraft
) =>
  ({
    type: SET_CURRENT_RULE,
    payload: {status, rule},
  } as const)

export const removeRule = (ruleID: string) => ({
  type: REMOVE_RULE,
  payload: {ruleID},
})

export const addLabelToRule = (ruleID: string, label: Label) =>
  ({
    type: ADD_LABEL_TO_RULE,
    ruleID,
    label,
  } as const)

export const removeLabelFromRule = (ruleID: string, labelID: string) =>
  ({
    type: REMOVE_LABEL_FROM_RULE,
    ruleID,
    labelID,
  } as const)
