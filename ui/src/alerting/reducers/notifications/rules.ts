// Libraries
import {produce} from 'immer'

// Types
import {RemoteDataState, NotificationRuleDraft} from 'src/types'
import {Action} from 'src/alerting/actions/notifications/rules'

export interface NotificationRulesState {
  status: RemoteDataState
  list: NotificationRuleDraft[]
  current: {status: RemoteDataState; rule: NotificationRuleDraft}
}

export const defaultNotificationRulesState: NotificationRulesState = {
  status: RemoteDataState.NotStarted,
  list: [],
  current: {status: RemoteDataState.NotStarted, rule: null},
}

export default (
  state: NotificationRulesState = defaultNotificationRulesState,
  action: Action
): NotificationRulesState =>
  produce(state, draftState => {
    switch (action.type) {
      case 'SET_ALL_NOTIFICATION_RULES':
        const {status, rules} = action.payload
        draftState.status = status
        if (rules) {
          draftState.list = rules
        }
        return

      case 'SET_NOTIFICATION_RULE':
        const newNotificationRule = action.payload.rule
        const ruleIndex = state.list.findIndex(
          nr => nr.id == newNotificationRule.id
        )

        if (ruleIndex == -1) {
          draftState.list.push(newNotificationRule)
        } else {
          draftState.list[ruleIndex] = newNotificationRule
        }
        return

      case 'REMOVE_NOTIFICATION_RULE':
        const {ruleID} = action.payload
        draftState.list = draftState.list.filter(nr => nr.id != ruleID)
        return

      case 'SET_CURRENT_NOTIFICATION_RULE':
        draftState.current.status = action.payload.status
        if (action.payload.rule) {
          draftState.current.rule = action.payload.rule
        }
        return
    }
  })
