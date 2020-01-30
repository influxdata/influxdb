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
import {
  Action,
  setRules,
  setRule,
  setCurrentRule,
  removeRule,
  addLabelToRule,
  removeLabelFromRule,
} from 'src/notifications/rules/actions/creators'
import {checkRulesLimits} from 'src/cloud/actions/limits'

// Utils
import {
  ruleToDraftRule,
  draftRuleToPostRule,
} from 'src/notifications/rules/utils'
import {getOrg} from 'src/organizations/selectors'

// Types
import {
  NotificationRuleUpdate,
  GetState,
  NotificationRuleDraft,
  Label,
  RemoteDataState,
} from 'src/types'
import {incrementCloneName} from 'src/utils/naming'

export const getNotificationRules = () => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkRulesLimits>
  >,
  getState: GetState
) => {
  try {
    dispatch(setRules(RemoteDataState.Loading))
    const {id: orgID} = getOrg(getState())
    const resp = await api.getNotificationRules({query: {orgID}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const draftRules = resp.data.notificationRules.map(ruleToDraftRule)

    dispatch(setRules(RemoteDataState.Done, draftRules))
    dispatch(checkRulesLimits())
  } catch (error) {
    console.error(error)
    dispatch(setRules(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRulesFailed(error.message)))
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

    dispatch(setCurrentRule(RemoteDataState.Done, ruleToDraftRule(resp.data)))
  } catch (error) {
    console.error(error)
    dispatch(setCurrentRule(RemoteDataState.Error))
    dispatch(notify(copy.getNotificationRuleFailed(error.message)))
  }
}

export const createRule = (rule: NotificationRuleDraft) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkRulesLimits>
  >
) => {
  const data = draftRuleToPostRule(rule)

  try {
    const resp = await api.postNotificationRule({data})

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(setRule(ruleToDraftRule(resp.data)))
    dispatch(checkRulesLimits())
  } catch (error) {
    console.error(error)
  }
}

export const updateRule = (rule: NotificationRuleDraft) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  if (rule.offset == '') {
    throw new Error('Notification Rule offset field can not be empty')
  }
  if (rule.every == '') {
    throw new Error('Notification Rule every field can not be empty')
  }

  try {
    const resp = await api.putNotificationRule({
      ruleID: rule.id,
      data: draftRuleToPostRule(rule),
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setRule(ruleToDraftRule(resp.data)))
  } catch (error) {
    console.error(error)
  }
}

export const updateRuleProperties = (
  ruleID: string,
  properties: NotificationRuleUpdate
) => async (dispatch: Dispatch<Action | NotificationAction>) => {
  try {
    const resp = await api.patchNotificationRule({
      ruleID,
      data: properties,
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    dispatch(setRule(ruleToDraftRule(resp.data)))
  } catch (error) {
    console.error(error)
  }
}

export const deleteRule = (ruleID: string) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkRulesLimits>
  >
) => {
  try {
    const resp = await api.deleteNotificationRule({ruleID})

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeRule(ruleID))
    dispatch(checkRulesLimits())
  } catch (error) {
    console.error(error)
  }
}

export const addRuleLabel = (ruleID: string, label: Label) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.postNotificationRulesLabel({
      ruleID,
      data: {labelID: label.id},
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(addLabelToRule(ruleID, label))
  } catch (error) {
    console.error(error)
  }
}

export const deleteRuleLabel = (ruleID: string, labelID: string) => async (
  dispatch: Dispatch<Action | NotificationAction>
) => {
  try {
    const resp = await api.deleteNotificationRulesLabel({
      ruleID,
      labelID,
    })

    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }

    dispatch(removeLabelFromRule(ruleID, labelID))
  } catch (error) {
    console.error(error)
  }
}

export const cloneRule = (draftRule: NotificationRuleDraft) => async (
  dispatch: Dispatch<
    Action | NotificationAction | ReturnType<typeof checkRulesLimits>
  >,
  getState: GetState
): Promise<void> => {
  try {
    const {
      rules: {list},
    } = getState()

    const rule = draftRuleToPostRule(draftRule)

    const allRuleNames = list.map(r => r.name)

    const clonedName = incrementCloneName(allRuleNames, rule.name)

    const resp = await api.postNotificationRule({
      data: {...rule, name: clonedName},
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    dispatch(setRule(ruleToDraftRule(resp.data)))
    dispatch(checkRulesLimits())
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createRuleFailed(error.message)))
  }
}
