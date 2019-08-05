// Libraries
import {v4} from 'uuid'

// Types
import {NotificationRuleUI, StatusRuleItem, TagRuleItem} from 'src/types'

export type RuleState = NotificationRuleUI
export type Actions =
  | {type: 'UPDATE_RULE'; rule: NotificationRuleUI}
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}
  | {type: 'UPDATE_STATUS_RULES'; statusRule: StatusRuleItem}
  | {type: 'ADD_STATUS_RULE'; statusRule: StatusRuleItem}
  | {type: 'ADD_TAG_RULE'; tagRule: TagRuleItem}
  | {type: 'DELETE_STATUS_RULE'; statusRuleID: string}
  | {type: 'UPDATE_TAG_RULES'; tagRule: TagRuleItem}

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

    case 'ADD_STATUS_RULE': {
      const {statusRule} = action
      return {
        ...state,
        statusRules: [...state.statusRules, {...statusRule, id: v4()}],
      }
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

    default:
      throw new Error('unhandled reducer action in <NewRuleOverlay/>')
  }
}
