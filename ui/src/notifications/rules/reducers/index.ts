// Libraries
import {produce} from 'immer'

// Types
import {
  RemoteDataState,
  RulesState,
  NotificationRule,
  ResourceType,
} from 'src/types'
import {
  Action,
  SET_RULES,
  SET_RULE,
  REMOVE_RULE,
  SET_CURRENT_RULE,
  REMOVE_LABEL_FROM_RULE,
} from 'src/notifications/rules/actions/creators'
import {
  setResource,
  removeResource,
  setResourceAtID,
  setRelation,
} from 'src/resources/reducers/helpers'
import {SET_LABEL_ON_RESOURCE} from 'src/labels/actions/creators'

export const defaultNotificationRulesState: RulesState = {
  status: RemoteDataState.NotStarted,
  byID: {},
  allIDs: [],
  current: {status: RemoteDataState.NotStarted, rule: null},
}

export default (
  state: RulesState = defaultNotificationRulesState,
  action: Action
): RulesState =>
  produce(state, draftState => {
    switch (action.type) {
      case SET_RULES: {
        setResource<NotificationRule>(
          draftState,
          action,
          ResourceType.NotificationRules
        )

        return
      }

      case SET_RULE: {
        setResourceAtID<NotificationRule>(
          draftState,
          action,
          ResourceType.NotificationRules
        )

        return
      }

      case REMOVE_RULE: {
        removeResource<NotificationRule>(draftState, action)

        return
      }

      case SET_CURRENT_RULE: {
        const {schema, status} = action
        const ruleID = schema.result

        draftState.current.status = status
        const rule = schema.entities.rules[ruleID]

        if (rule) {
          draftState.current.rule = rule
        }

        return
      }

      case SET_LABEL_ON_RESOURCE: {
        const {resourceID, schema} = action
        const labelID = schema.result

        setRelation<NotificationRule>(
          draftState,
          ResourceType.Labels,
          labelID,
          resourceID
        )

        return
      }

      case REMOVE_LABEL_FROM_RULE: {
        const {ruleID, labelID} = action
        const labels = draftState.byID[ruleID].labels

        draftState.byID[ruleID].labels = labels.filter(
          label => label.id !== labelID
        )

        return
      }
    }
  })
