// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, NotificationRule} from 'src/types'
import {Action} from 'src/alerting/actions/notificationRules'

export interface NotificationRulesState {
  status: RemoteDataState
  list: NotificationRule[]
  current: {status: RemoteDataState; notificationRule: NotificationRule}
}

export const defaultNotificationRulesState: NotificationRulesState = {
  status: RemoteDataState.NotStarted,
  list: [],
  current: {status: RemoteDataState.NotStarted, notificationRule: null},
}

export default (
  state: NotificationRulesState = defaultNotificationRulesState,
  action: Action
): NotificationRulesState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_ALL_NOTIFICATION_RULES':
        const {status, notificationRules} = action.payload
        draftState.status = status
        if (notificationRules) {
          draftState.list = notificationRules
        }
        return

      case 'SET_NOTIFICATION_RULE':
        const newNotificationRule = action.payload.notificationRule
        const notificationRuleIndex = state.list.findIndex(
          nr => nr.id == newNotificationRule.id
        )

        if (notificationRuleIndex == -1) {
          draftState.list.push(newNotificationRule)
        } else {
          draftState.list[notificationRuleIndex] = newNotificationRule
        }
        return

      case 'REMOVE_NOTIFICATION_RULE':
        const {notificationRuleID} = action.payload
        draftState.list = draftState.list.filter(
          nr => nr.id != notificationRuleID
        )
        return

      case 'SET_CURRENT_NOTIFICATION_RULE':
        draftState.current.status = action.payload.status
        if (action.payload.notificationRule) {
          draftState.current.notificationRule = action.payload.notificationRule
        }
        return
    }
  })
