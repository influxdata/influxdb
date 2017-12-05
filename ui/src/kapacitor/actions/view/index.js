import uuid from 'node-uuid'
import {getActiveKapacitor} from 'shared/apis'
import {publishNotification} from 'shared/actions/notifications'
import {
  getRules,
  getRule as getRuleAJAX,
  deleteRule as deleteRuleAPI,
  updateRuleStatus as updateRuleStatusAPI,
  createTask as createTaskAJAX,
  updateTask as updateTaskAJAX,
} from 'src/kapacitor/apis'
import {errorThrown} from 'shared/actions/errors'

const loadQuery = query => ({
  type: 'KAPA_LOAD_QUERY',
  payload: {
    query,
  },
})

export function fetchRule(source, ruleID) {
  return dispatch => {
    getActiveKapacitor(source).then(kapacitor => {
      getRuleAJAX(kapacitor, ruleID).then(({data: rule}) => {
        dispatch({
          type: 'LOAD_RULE',
          payload: {
            rule: Object.assign(rule, {queryID: rule.query.id}),
          },
        })
        dispatch(loadQuery(rule.query))
      })
    })
  }
}

const addQuery = queryID => ({
  type: 'KAPA_ADD_QUERY',
  payload: {
    queryID,
  },
})

export const getRule = (kapacitor, ruleID) => async dispatch => {
  try {
    const {data: rule} = await getRuleAJAX(kapacitor, ruleID)

    dispatch({
      type: 'LOAD_RULE',
      payload: {
        rule: {...rule, queryID: rule.query && rule.query.id},
      },
    })

    if (rule.query) {
      dispatch({
        type: 'LOAD_KAPACITOR_QUERY',
        payload: {
          query: rule.query,
        },
      })
    }
  } catch (error) {
    console.error(error)
    throw error
  }
}

export function loadDefaultRule() {
  return dispatch => {
    const queryID = uuid.v4()
    dispatch({
      type: 'LOAD_DEFAULT_RULE',
      payload: {
        queryID,
      },
    })
    dispatch(addQuery(queryID))
  }
}

export const fetchRules = kapacitor => async dispatch => {
  try {
    const {data: {rules}} = await getRules(kapacitor)
    dispatch({type: 'LOAD_RULES', payload: {rules}})
  } catch (error) {
    dispatch(errorThrown(error))
  }
}

export function chooseTrigger(ruleID, trigger) {
  return {
    type: 'CHOOSE_TRIGGER',
    payload: {
      ruleID,
      trigger,
    },
  }
}

export const addEvery = (ruleID, frequency) => ({
  type: 'ADD_EVERY',
  payload: {
    ruleID,
    frequency,
  },
})

export const removeEvery = ruleID => ({
  type: 'REMOVE_EVERY',
  payload: {
    ruleID,
  },
})

export function updateRuleValues(ruleID, trigger, values) {
  return {
    type: 'UPDATE_RULE_VALUES',
    payload: {
      ruleID,
      trigger,
      values,
    },
  }
}

export function updateMessage(ruleID, message) {
  return {
    type: 'UPDATE_RULE_MESSAGE',
    payload: {
      ruleID,
      message,
    },
  }
}

export function updateDetails(ruleID, details) {
  return {
    type: 'UPDATE_RULE_DETAILS',
    payload: {
      ruleID,
      details,
    },
  }
}

export const updateAlertProperty = (ruleID, alertNodeName, alertProperty) => ({
  type: 'UPDATE_RULE_ALERT_PROPERTY',
  payload: {
    ruleID,
    alertNodeName,
    alertProperty,
  },
})

export function updateAlertNodes(ruleID, alerts) {
  return {
    type: 'UPDATE_RULE_ALERT_NODES',
    payload: {ruleID, alerts},
  }
}

export function updateRuleName(ruleID, name) {
  return {
    type: 'UPDATE_RULE_NAME',
    payload: {
      ruleID,
      name,
    },
  }
}

export function deleteRuleSuccess(ruleID) {
  return {
    type: 'DELETE_RULE_SUCCESS',
    payload: {
      ruleID,
    },
  }
}

export function updateRuleStatusSuccess(ruleID, status) {
  return {
    type: 'UPDATE_RULE_STATUS_SUCCESS',
    payload: {
      ruleID,
      status,
    },
  }
}

export function deleteRule(rule) {
  return dispatch => {
    deleteRuleAPI(rule)
      .then(() => {
        dispatch(deleteRuleSuccess(rule.id))
        dispatch(
          publishNotification('success', `${rule.name} deleted successfully`)
        )
      })
      .catch(() => {
        dispatch(
          publishNotification('error', `${rule.name} could not be deleted`)
        )
      })
  }
}

export function updateRuleStatus(rule, status) {
  return dispatch => {
    updateRuleStatusAPI(rule, status)
      .then(() => {
        dispatch(
          publishNotification('success', `${rule.name} ${status} successfully`)
        )
      })
      .catch(() => {
        dispatch(
          publishNotification('error', `${rule.name} could not be ${status}`)
        )
      })
  }
}

export const createTask = (
  kapacitor,
  task,
  router,
  sourceID
) => async dispatch => {
  try {
    const {data} = await createTaskAJAX(kapacitor, task)
    router.push(`/sources/${sourceID}/alert-rules`)
    dispatch(publishNotification('success', 'You made a TICKscript!'))
    return data
  } catch (error) {
    if (!error) {
      dispatch(errorThrown('Could not communicate with server'))
      return
    }

    return error.data
  }
}

export const updateTask = (
  kapacitor,
  task,
  ruleID,
  router,
  sourceID
) => async dispatch => {
  try {
    const {data} = await updateTaskAJAX(kapacitor, task, ruleID, sourceID)
    router.push(`/sources/${sourceID}/alert-rules`)
    dispatch(publishNotification('success', 'TICKscript updated successully'))
    return data
  } catch (error) {
    if (!error) {
      dispatch(errorThrown('Could not communicate with server'))
      return
    }

    return error.data
  }
}
