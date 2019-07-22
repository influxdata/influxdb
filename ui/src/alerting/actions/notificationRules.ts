// Libraries
import {client} from 'src/utils/api'
import {Dispatch} from 'react'

// Constants
import * as copy from 'src/shared/copy/notifications'

//Actions
import {
  notify,
  Action as NotificationAction,
} from 'src/shared/actions/notifications'

// Types
import {RemoteDataState} from '@influxdata/clockface'
import {NotificationRule, GetState} from 'src/types'

export type Action =
  | ReturnType<typeof setAllNotificationRules>
  | ReturnType<typeof setNotificationRulesStatus>
  | ReturnType<typeof setNotificationRule>
  | ReturnType<typeof setNotificationRuleStatus>

const setAllNotificationRules = (
  status: RemoteDataState,
  notificationRules?: NotificationRule[]
) => ({
  type: 'SET_ALL_NOTIFICATIONRULES' as 'SET_ALL_NOTIFICATIONRULES',
  payload: {status, notificationRules},
})

const setNotificationRulesStatus = (status: RemoteDataState) => ({
  type: 'SET_NOTIFICATIONRULES_STATUS' as 'SET_NOTIFICATIONRULES_STATUS',
  payload: {status},
})

const setNotificationRule = (
  status: RemoteDataState,
  notificationRule?: NotificationRule
) => ({
  type: 'SET_NOTIFICATIONRULE' as 'SET_NOTIFICATIONRULE',
  payload: {status, notificationRule},
})

const setNotificationRuleStatus = (status: RemoteDataState) => ({
  type: 'SET_NOTIFICATIONRULE_STATUS' as 'SET_NOTIFICATIONRULE_STATUS',
  payload: {status},
})

export const getNotificationRules = () => async (
  dispatch: Dispatch<Action | NotificationAction>,
  getState: GetState
) => {
  try {
    dispatch(setNotificationRulesStatus(RemoteDataState.Loading))
    const {
      orgs: {
        org: {id: orgID},
      },
    } = getState()

    const notificationRules = await client.notificationRules.getAll(orgID)

    dispatch(setAllNotificationRules(RemoteDataState.Done, notificationRules))
  } catch (e) {
    console.error(e)
    dispatch(setNotificationRulesStatus(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRulesFailed(e.message)))
  }
}

export const getNotificationRule = (notificationRuleID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    dispatch(setNotificationRuleStatus(RemoteDataState.Loading))

    const notificationRule = await client.notificationRules.get(
      notificationRuleID
    )

    dispatch(setNotificationRule(RemoteDataState.Done, notificationRule))
  } catch (e) {
    console.error(e)
    dispatch(setNotificationRuleStatus(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRuleFailed(e.message)))
  }
}
