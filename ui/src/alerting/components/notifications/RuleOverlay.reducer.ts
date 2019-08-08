// Libraries
import {useCallback, useReducer} from 'react'
import {v4} from 'uuid'
import {omit} from 'lodash'

// Types
import {
  NotificationRuleDraft,
  StatusRuleDraft,
  TagRuleDraft,
  CheckStatusLevel,
} from 'src/types'

// Hooks
import {RuleMode} from 'src/shared/hooks'

export type LevelType = 'currentLevel' | 'previousLevel'
export type RuleState = NotificationRuleDraft
export type ActionMode = {mode: RuleMode}
export type ActionPayload =
  | {type: 'UPDATE_RULE'; rule: NotificationRuleDraft}
  | {
      type: 'UPDATE_STATUS_LEVEL'
      statusID: string
      levelType: LevelType
      level: CheckStatusLevel
    }
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}
  | {type: 'UPDATE_STATUS_RULES'; statusRule: StatusRuleDraft}
  | {type: 'ADD_TAG_RULE'; tagRule: TagRuleDraft}
  | {type: 'DELETE_STATUS_RULE'; statusRuleID: string}
  | {type: 'UPDATE_TAG_RULES'; tagRule: TagRuleDraft}
  | {type: 'DELETE_TAG_RULE'; tagRuleID: string}
  | {
      type: 'SET_TAG_RULE_OPERATOR'
      tagRuleID: string
      operator: TagRuleDraft['value']['operator']
    }

export type Action = ActionPayload & ActionMode

export const reducer = (mode: RuleMode) => (
  state: RuleState,
  action: Action
) => {
  if (mode !== action.mode) {
    return state
  }

  switch (action.type) {
    case 'UPDATE_RULE': {
      const {rule} = action
      return {...state, ...rule}
    }

    case 'SET_ACTIVE_SCHEDULE': {
      const {schedule} = action
      let newState: RuleState = state

      if (schedule === 'every') {
        newState = omit(state, 'cron') as RuleState
      }

      if (schedule === 'cron') {
        newState = omit<RuleState>(state, 'every') as RuleState
      }

      return {...newState, [schedule]: ''}
    }

    case 'UPDATE_STATUS_RULES': {
      const {statusRule} = action
      const statusRules = state.statusRules.map(s => {
        if (s.id !== statusRule.id) {
          return s
        }

        return statusRule
      })

      return {...state, statusRules}
    }

    case 'ADD_TAG_RULE': {
      const {tagRule} = action
      return {
        ...state,
        tagRules: [...state.tagRules, {...tagRule, id: v4()}],
      }
    }

    case 'UPDATE_TAG_RULES': {
      const {tagRule} = action
      const tagRules = state.tagRules.map(t => {
        if (t.id !== tagRule.id) {
          return t
        }

        return tagRule
      })

      return {...state, tagRules}
    }

    case 'DELETE_STATUS_RULE': {
      const {statusRuleID} = action
      const statusRules = state.statusRules.filter(s => {
        return s.id !== statusRuleID
      })

      return {
        ...state,
        statusRules,
      }
    }

    case 'DELETE_TAG_RULE': {
      const {tagRuleID} = action

      const tagRules = state.tagRules.filter(tr => {
        return tr.id !== tagRuleID
      })

      return {...state, tagRules}
    }

    case 'UPDATE_STATUS_LEVEL': {
      const {levelType, level, statusID} = action

      const statusRules = state.statusRules.map(status => {
        if (status.id !== statusID) {
          return status
        }

        const value = {
          ...status.value,
          [levelType]: {
            ...status.value[levelType],
            level,
          },
        }

        return {...status, value}
      })

      return {...state, statusRules}
    }

    case 'SET_TAG_RULE_OPERATOR': {
      const {tagRuleID, operator} = action
      const tagRules = state.tagRules.map(tagRule => {
        if (tagRule.id !== tagRuleID) {
          return tagRule
        }

        return {
          ...tagRule,
          value: {
            ...tagRule.value,
            operator,
          },
        }
      })

      return {...state, tagRules}
    }

    default:
      const neverAction: never = action
      throw new Error(
        `Unhandled action: "${neverAction}" in RuleOverlay.reducer.ts`
      )
  }
}

export const memoizedReducer = (mode: RuleMode, state) => {
  const memo = useCallback(reducer(mode), [mode])
  return useReducer(memo, state)
}
