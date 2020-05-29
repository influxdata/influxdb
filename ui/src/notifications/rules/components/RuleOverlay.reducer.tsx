// Libraries
import {v4} from 'uuid'
import {omit} from 'lodash'

// Types
import {NotificationRuleDraft} from 'src/types'
import {Action} from './RuleOverlay.actions'

export type LevelType = 'currentLevel' | 'previousLevel'
export type RuleState = NotificationRuleDraft

export const reducer = (state: RuleState, action: Action) => {
  switch (action.type) {
    case 'UPDATE_RULE': {
      const {rule} = action
      return {...state, ...rule}
    }

    case 'SET_ACTIVE_SCHEDULE': {
      const {schedule} = action
      let newState: RuleState = state

      if (schedule === 'every') {
        newState = omit(state, 'cron') as NotificationRuleDraft
      }

      if (schedule === 'cron') {
        newState = omit(state, 'every') as NotificationRuleDraft
      }

      return {...newState, [schedule]: ''}
    }

    case 'UPDATE_STATUS_RULES': {
      const {statusRule} = action
      const statusRules = state.statusRules.map(s => {
        if (s.cid !== statusRule.cid) {
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
        if (t.cid !== tagRule.cid) {
          return t
        }

        return tagRule
      })

      return {...state, tagRules}
    }

    case 'DELETE_STATUS_RULE': {
      const {statusRuleID} = action
      const statusRules = state.statusRules.filter(s => {
        return s.cid !== statusRuleID
      })

      return {
        ...state,
        statusRules,
      }
    }

    case 'DELETE_TAG_RULE': {
      const {tagRuleID} = action

      const tagRules = state.tagRules.filter(tr => {
        return tr.cid !== tagRuleID
      })

      return {...state, tagRules}
    }

    case 'UPDATE_STATUS_LEVEL': {
      const {levelType, level, statusID} = action

      const statusRules = state.statusRules.map(status => {
        if (status.cid !== statusID) {
          return status
        }

        const value = {
          ...status.value,
          [levelType]: level,
        }

        return {...status, value}
      })

      return {...state, statusRules}
    }

    case 'SET_TAG_RULE_OPERATOR': {
      const {tagRuleID, operator} = action
      const tagRules = state.tagRules.map(tagRule => {
        if (tagRule.cid !== tagRuleID) {
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
        `Unhandled action "${
          (neverAction as any).type
        }" in RuleOverlay.reducer.ts`
      )
  }
}
