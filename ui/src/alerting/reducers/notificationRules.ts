// Types
import {RemoteDataState, NotificationRule} from 'src/types'
import {Action} from 'src/alerting/actions/notificationRules'

export interface NotificationRulesState {
  status: RemoteDataState
  list: NotificationRule[]
  current: {status: RemoteDataState; notificationRule: NotificationRule}
}

export const defaultNotificationRuleState: NotificationRulesState = {
  status: RemoteDataState.NotStarted,
  list: [],
  current: {status: RemoteDataState.NotStarted, notificationRule: null},
}

export default (
  state: NotificationRulesState = defaultNotificationRuleState,
  action: Action
): NotificationRulesState => {
  switch (action.type) {
    case 'SET_NOTIFICATIONRULES_STATUS':
      return {
        ...state,
        status: action.payload.status,
      }
    case 'SET_ALL_NOTIFICATIONRULES':
      return {
        ...state,
        list: action.payload.notificationRules,
        status: RemoteDataState.Done,
      }
    case 'SET_NOTIFICATIONRULE_STATUS':
      return {
        ...state,
        current: {...state.current, status: action.payload.status},
      }
    case 'SET_NOTIFICATIONRULE':
      return {
        ...state,
        current: {
          status: action.payload.status,
          notificationRule: action.payload.notificationRule,
        },
      }

    default:
      return state
  }
}
