// Libraries
import {v4} from 'uuid'

// Types
import {NotificationRuleDraft, StatusRuleDraft, TagRuleDraft} from 'src/types'

export type RuleState = NotificationRuleDraft
export type Actions =
  | {type: 'UPDATE_RULE'; rule: NotificationRuleDraft}
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}
  | {type: 'UPDATE_STATUS_RULES'; statusRule: StatusRuleDraft}
  | {type: 'ADD_TAG_RULE'; tagRule: TagRuleDraft}
  | {type: 'DELETE_STATUS_RULE'; statusRuleID: string}
  | {type: 'UPDATE_TAG_RULES'; tagRule: TagRuleDraft}
  | {type: 'DELETE_TAG_RULE'; tagRuleID: string}

export const reducer = (state: RuleState, action: Actions) => {
  switch (action.type) {
    case 'UPDATE_RULE': {
      const {rule} = action
      return {...state, ...rule}
    }

    case 'SET_ACTIVE_SCHEDULE': {
      const {schedule} = action
      return {...state, schedule}
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

    default:
      throw new Error('unhandled reducer action in <NewRuleOverlay/>')
  }
}
