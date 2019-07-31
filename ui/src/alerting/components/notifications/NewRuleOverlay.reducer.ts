// Libraries
import {v4} from 'uuid'

// Types
import {NotificationRuleUI, StatusRuleItem, TagRuleItem} from 'src/types'

export type RuleState = NotificationRuleUI
export type Actions =
  | {type: 'UPDATE_RULE'; rule: NotificationRuleUI}
  | {type: 'SET_ACTIVE_SCHEDULE'; schedule: 'cron' | 'every'}
  | {type: 'UPDATE_STATUS_RULES'; status: StatusRuleItem}
  | {type: 'ADD_STATUS_RULE'; statusRule: StatusRuleItem}
  | {type: 'ADD_TAG_RULE'; tagRule: TagRuleItem}

export const reducer = (state: RuleState, action: Actions) => {
  switch (action.type) {
    case 'UPDATE_RULE':
      const {rule} = action
      return {...state, ...rule}
    case 'SET_ACTIVE_SCHEDULE':
      const {schedule} = action
      return {...state, schedule}
    case 'UPDATE_STATUS_RULES':
      const {status} = action
      const statusRules = state.statusRules.map(s => {
        if (s.id !== status.id) {
          return s
        }

        return status
      })

      return {...state, statusRules}
    case 'ADD_STATUS_RULE':
      const {statusRule} = action
      return {
        ...state,
        statusRules: [...state.statusRules, {...statusRule, id: v4()}],
      }
    case 'ADD_TAG_RULE':
      const {tagRule} = action
      return {
        ...state,
        tagRules: [...state.tagRules, {...tagRule, id: v4()}],
      }

    default:
      throw new Error('unhandled reducer action in <NewRuleOverlay/>')
  }
}
