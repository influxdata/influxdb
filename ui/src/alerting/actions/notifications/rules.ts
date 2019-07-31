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
  | ReturnType<typeof setRule>
  | ReturnType<typeof setCurrentRule>
  | ReturnType<typeof removeRule>

export const setAllNotificationRules = (
  status: RemoteDataState,
  rules?: NotificationRule[]
) => ({
  type: 'SET_ALL_NOTIFICATION_RULES' as 'SET_ALL_NOTIFICATION_RULES',
  payload: {status, rules},
})

export const setRule = (rule: NotificationRule) => ({
  type: 'SET_NOTIFICATION_RULE' as 'SET_NOTIFICATION_RULE',
  payload: {rule},
})

export const setCurrentRule = (
  status: RemoteDataState,
  rule?: NotificationRule
) => ({
  type: 'SET_CURRENT_NOTIFICATION_RULE' as 'SET_CURRENT_NOTIFICATION_RULE',
  payload: {status, rule},
})

export const removeRule = (ruleID: string) => ({
  type: 'REMOVE_NOTIFICATION_RULE' as 'REMOVE_NOTIFICATION_RULE',
  payload: {ruleID},
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

export const getCurrentRule = (ruleID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    dispatch(setCurrentRule(RemoteDataState.Loading))

    const resp = await api.getNotificationRule({ruleID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setCurrentRule(RemoteDataState.Done, resp.data))
  } catch (e) {
    console.error(e)
    dispatch(setCurrentRule(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRuleFailed(e.message)))
  }
}

export const createRule = (rule: Partial<NotificationRule>) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.postNotificationRule({
      data: rule as NotificationRule,
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.createRuleFailed(e.message)))
  }
}

export const updateRule = (rule: Partial<NotificationRule>) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.putNotificationRule({
      ruleID: rule.id,
      data: rule as NotificationRule,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setRule(resp.data))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.updateRuleFailed(e.message)))
  }
}

export const deleteRule = (ruleID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteNotificationRule({ruleID: ruleID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeRule(ruleID))
  } catch (e) {
    console.error(e)
    dispatch(notify(copy.deleteRuleFailed(e.message)))
  }
}
