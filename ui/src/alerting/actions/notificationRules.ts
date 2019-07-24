// Libraries
import {client} from 'src/utils/api'
import {Dispatch} from 'react'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'

// Types
import {RemoteDataState} from '@influxdata/clockface'
import {NotificationRule, GetState} from 'src/types'

export type Action =
  | ReturnType<typeof setAllNotificationRules>
  | ReturnType<typeof setNotificationRule>
  | ReturnType<typeof setCurrentNotificationRule>
  | ReturnType<typeof removeNotificationRule>

export const setAllNotificationRules = (
  status: RemoteDataState,
  notificationRules?: NotificationRule[]
) => ({
  type: 'SET_ALL_NOTIFICATION_RULES' as 'SET_ALL_NOTIFICATION_RULES',
  payload: {status, notificationRules},
})

export const setNotificationRule = (notificationRule: NotificationRule) => ({
  type: 'SET_NOTIFICATION_RULE' as 'SET_NOTIFICATION_RULE',
  payload: {notificationRule},
})

export const setCurrentNotificationRule = (
  status: RemoteDataState,
  notificationRule?: NotificationRule
) => ({
  type: 'SET_CURRENT_NOTIFICATION_RULE' as 'SET_CURRENT_NOTIFICATION_RULE',
  payload: {status, notificationRule},
})

export const removeNotificationRule = (notificationRuleID: string) => ({
  type: 'REMOVE_NOTIFICATION_RULE' as 'REMOVE_NOTIFICATION_RULE',
  payload: {notificationRuleID},
})

export const getNotificationRules = () => async (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => {
  try {
    dispatch(setAllNotificationRules(RemoteDataState.Loading))
    const {
      orgs: {
        org: {id: orgID},
      },
    } = getState()

    const notificationRules = (await client.notificationRules.getAll(
      orgID
    )) as NotificationRule[]

    dispatch(setAllNotificationRules(RemoteDataState.Done, notificationRules))
  } catch (e) {
    console.error(e)
    dispatch(setAllNotificationRules(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRulesFailed(e.message)))
  }
}

export const getCurrentNotificationRule = (
  notificationRuleID: string
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    dispatch(setCurrentNotificationRule(RemoteDataState.Loading))

    const notificationRule = (await client.notificationRules.get(
      notificationRuleID
    )) as NotificationRule

    dispatch(setCurrentNotificationRule(RemoteDataState.Done, notificationRule))
  } catch (e) {
    console.error(e)
    dispatch(setCurrentNotificationRule(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRuleFailed(e.message)))
  }
}

export const createNotificationRule = (
  notificationRule: NotificationRule
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    client.notificationRules.create(notificationRule)
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createNotificationRuleFailed(e.message)))
  }
}

export const updateNotificationRule = (
  notificationRule: Partial<NotificationRule>
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const updatedNotificationRule = (await client.notificationRules.update(
      notificationRule.id,
      notificationRule
    )) as NotificationRule
    dispatch(setNotificationRule(updatedNotificationRule))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateNotificationRuleFailed(e.message)))
  }
}

export const deleteNotificationRule = (notificationRuleID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    await client.notificationRules.delete(notificationRuleID)
    dispatch(removeNotificationRule(notificationRuleID))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteNotificationRuleFailed(e.message)))
  }
}
