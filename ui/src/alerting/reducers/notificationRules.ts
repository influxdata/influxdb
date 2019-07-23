// Types
import {RemoteDataState, NotificationRule} from 'src/types'
import {Action} from 'src/alerting/actions/notificationRules'

import {notificationRule} from 'src/alerting/constants'

export interface NotificationRulesState {
  status: RemoteDataState
  list: NotificationRule[]
  current: {status: RemoteDataState; notificationRule: NotificationRule}
}

export const defaultNotificationRulesState: NotificationRulesState = {
  status: RemoteDataState.NotStarted,
  list: [notificationRule],
  current: {status: RemoteDataState.NotStarted, notificationRule: null},
}

export default (
  state: NotificationRulesState = defaultNotificationRulesState,
  action: Action
): NotificationRulesState => {
  switch (action.type) {
    case 'SET_ALL_NOTIFICATION_RULES':
      if (action.payload.notificationRules) {
        return {
          ...state,
          list: action.payload.notificationRules,
          status: action.payload.status,
        }
      }
      return {
        ...state,
        status: action.payload.status,
      }
    case 'SET_NOTIFICATION_RULE':
      const newNotificationRule = action.payload.notificationRule
      const notificationRuleIndex = state.list.findIndex(
        nr => nr.id == newNotificationRule.id
      )

      let updatedList = state.list
      if (notificationRuleIndex == -1) {
        updatedList = [...updatedList, newNotificationRule]
      } else {
        updatedList[notificationRuleIndex] = newNotificationRule
      }

      return {
        ...state,
        list: updatedList,
      }
    case 'SET_CURRENT_NOTIFICATION_RULE':
      if (action.payload.notificationRule) {
        return {
          ...state,
          current: {
            notificationRule: action.payload.notificationRule,
            status: action.payload.status,
          },
        }
      }
      return {
        ...state,
        current: {...state.current, status: action.payload.status},
      }
    case 'REMOVE_NOTIFICATION_RULE':
      const list = state.list.filter(
        c => c.id != action.payload.notificationRuleID
      )
      return {
        ...state,
        list: list,
      }
    default:
      return state
  }
}
