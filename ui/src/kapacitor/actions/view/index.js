import uuid from 'uuid'
import {getActiveKapacitor} from 'shared/apis'
import {notify} from 'shared/actions/notifications'
import {
  getRules,
  getRule as getRuleAJAX,
  deleteRule as deleteRuleAPI,
  updateRuleStatus as updateRuleStatusAPI,
  createTask as createTaskAJAX,
  updateTask as updateTaskAJAX,
} from 'src/kapacitor/apis'
import {errorThrown} from 'shared/actions/errors'

import {
  NOTIFY_ALERT_RULE_DELETED,
  NOTIFY_ALERT_RULE_DELETION_FAILED,
  NOTIFY_ALERT_RULE_STATUS_UPDATED,
  NOTIFY_ALERT_RULE_STATUS_UPDATE_FAILED,
  NOTIFY_TICKSCRIPT_CREATED,
  NOTIFY_TICKSCRIPT_CREATION_FAILED,
  NOTIFY_TICKSCRIPT_UPDATED,
  NOTIFY_TICKSCRIPT_UPDATE_FAILED,
} from 'shared/copy/notifications'

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

export const loadDefaultRule = () => {
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

export const chooseTrigger = (ruleID, trigger) => ({
  type: 'CHOOSE_TRIGGER',
  payload: {
    ruleID,
    trigger,
  },
})

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

export const updateRuleValues = (ruleID, trigger, values) => ({
  type: 'UPDATE_RULE_VALUES',
  payload: {
    ruleID,
    trigger,
    values,
  },
})

export const updateMessage = (ruleID, message) => ({
  type: 'UPDATE_RULE_MESSAGE',
  payload: {
    ruleID,
    message,
  },
})

export const updateDetails = (ruleID, details) => ({
  type: 'UPDATE_RULE_DETAILS',
  payload: {
    ruleID,
    details,
  },
})

export function updateAlertNodes(ruleID, alerts) {
  return {
    type: 'UPDATE_RULE_ALERT_NODES',
    payload: {ruleID, alerts},
  }
}

export const updateRuleName = (ruleID, name) => ({
  type: 'UPDATE_RULE_NAME',
  payload: {
    ruleID,
    name,
  },
})

export const deleteRuleSuccess = ruleID => ({
  type: 'DELETE_RULE_SUCCESS',
  payload: {
    ruleID,
  },
})

export const updateRuleStatusSuccess = (ruleID, status) => ({
  type: 'UPDATE_RULE_STATUS_SUCCESS',
  payload: {
    ruleID,
    status,
  },
})

export const deleteRule = rule => dispatch => {
  deleteRuleAPI(rule)
    .then(() => {
      dispatch(deleteRuleSuccess(rule.id))
      dispatch(notify(NOTIFY_ALERT_RULE_DELETED(rule.name)))
    })
    .catch(() => {
      dispatch(notify(NOTIFY_ALERT_RULE_DELETION_FAILED(rule.name)))
    })
}

export const updateRuleStatus = (rule, status) => dispatch => {
  updateRuleStatusAPI(rule, status)
    .then(() => {
      dispatch(notify(NOTIFY_ALERT_RULE_STATUS_UPDATED(rule.name, status)))
    })
    .catch(() => {
      dispatch(
        notify(NOTIFY_ALERT_RULE_STATUS_UPDATE_FAILED(rule.name, status))
      )
    })
}

export const createTask = (kapacitor, task) => async dispatch => {
  try {
    const {data} = await createTaskAJAX(kapacitor, task)
    dispatch(notify(NOTIFY_TICKSCRIPT_CREATED))
    return data
  } catch (error) {
    if (!error) {
      dispatch(errorThrown(NOTIFY_TICKSCRIPT_CREATION_FAILED))
      return
    }

    return error.data
  }
}

export const updateTask = (
  kapacitor,
  task,
  ruleID,
  sourceID
) => async dispatch => {
  try {
    const {data} = await updateTaskAJAX(kapacitor, task, ruleID, sourceID)
    dispatch(notify(NOTIFY_TICKSCRIPT_UPDATED))
    return data
  } catch (error) {
    if (!error) {
      dispatch(errorThrown(NOTIFY_TICKSCRIPT_UPDATE_FAILED))
      return
    }
    return error.data
  }
}
