import {
  DEFAULT_SECONDS,
  READABLE_DEFAULT_SECONDS,
} from 'src/buckets/components/Retention'

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

export const DEFAULT_RULES: RetentionRule[] = [
  {type: 'expire' as 'expire', everySeconds: DEFAULT_SECONDS},
]

export const initialBucketState = (
  isRetentionLimitEnforced: boolean,
  orgID: string
) => ({
  name: '',
  retentionRules: isRetentionLimitEnforced ? DEFAULT_RULES : [],
  ruleType: isRetentionLimitEnforced ? ('expire' as 'expire') : null,
  readableRetention: isRetentionLimitEnforced
    ? READABLE_DEFAULT_SECONDS
    : 'forever',
  orgID,
  type: 'user' as 'user',
})

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
