export type RuleType = 'expire' | null

export interface RetentionRule {
  type: RuleType
  everySeconds: number
}

export interface ReducerState {
  name: string
  retentionRules: RetentionRule[]
  ruleType: RuleType
  readableRetention: string
  orgID: string
  type: 'user'
}

export type ReducerActionType =
  | 'updateName'
  | 'updateRuleType'
  | 'updateRetentionRules'
  | 'updateReadableRetention'

export interface Action {
  type: ReducerActionType
  payload: any
}

export const createBucketReducer = (state: ReducerState, action: Action) => {
  switch (action.type) {
    case 'updateName':
      return {...state, name: action.payload}
    case 'updateRuleType':
      return {...state, ruleType: action.payload}
    case 'updateRetentionRules':
      return {...state, retentionRules: action.payload}
    case 'updateReadableRetention':
      return {...state, readableRetention: action.payload}
  }
}
