// Libraries
import {Dispatch} from 'react'

// Constants
import * as copy from 'src/shared/copy/notifications'

// APIs
import * as api from 'src/client'

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

    const resp = await api.getNotificationRules({query: {orgID}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(
      setAllNotificationRules(RemoteDataState.Done, resp.data.notificationRules)
    )
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

    const resp = await api.getNotificationRule({ruleID: notificationRuleID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setCurrentNotificationRule(RemoteDataState.Done, resp.data))
  } catch (e) {
    console.error(e)
    dispatch(setCurrentNotificationRule(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRuleFailed(e.message)))
  }
}

export const createNotificationRule = (
  notificationRule: Partial<NotificationRule>
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.postNotificationRule({
      data: notificationRule as NotificationRule,
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createNotificationRuleFailed(e.message)))
  }
}

export const updateNotificationRule = (
  notificationRule: Partial<NotificationRule>
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.putNotificationRule({
      ruleID: notificationRule.id,
      data: notificationRule as NotificationRule,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setNotificationRule(resp.data))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateNotificationRuleFailed(e.message)))
  }
}

export const deleteNotificationRule = (notificationRuleID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteNotificationRule({ruleID: notificationRuleID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeNotificationRule(notificationRuleID))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteNotificationRuleFailed(e.message)))
  }
}
