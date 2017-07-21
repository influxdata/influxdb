import uuid from 'node-uuid'

import {getQueryConfig} from 'shared/apis'

import {errorThrown} from 'shared/actions/errors'
import {DEFAULT_DATA_EXPLORER_GROUP_BY_INTERVAL} from 'src/data_explorer/constants'

export function addQuery(options = {}) {
  return {
    type: 'ADD_QUERY',
    payload: {
      queryID: uuid.v4(),
      options,
    },
  }
}

export function deleteQuery(queryID) {
  return {
    type: 'DELETE_QUERY',
    payload: {
      queryID,
    },
  }
}

export function toggleField(queryId, fieldFunc, isKapacitorRule) {
  return {
    type: 'TOGGLE_FIELD',
    meta: {
      isKapacitorRule,
    },
    payload: {
      queryId,
      fieldFunc,
    },
  }
}

// all fields implicitly have a function applied to them, so consequently
// we need to set the auto group by time
export const toggleFieldWithGroupByInterval = (
  queryID,
  fieldFunc,
  isKapacitorRule
) => dispatch => {
  dispatch(toggleField(queryID, fieldFunc, isKapacitorRule))
  dispatch(groupByTime(queryID, DEFAULT_DATA_EXPLORER_GROUP_BY_INTERVAL))
}

export function groupByTime(queryId, time) {
  return {
    type: 'GROUP_BY_TIME',
    payload: {
      queryId,
      time,
    },
  }
}

export function applyFuncsToField(queryId, fieldFunc, isInDataExplorer) {
  return {
    type: 'APPLY_FUNCS_TO_FIELD',
    payload: {
      queryId,
      fieldFunc,
      isInDataExplorer,
    },
  }
}

export function chooseTag(queryId, tag) {
  return {
    type: 'CHOOSE_TAG',
    payload: {
      queryId,
      tag,
    },
  }
}

export function chooseNamespace(queryId, {database, retentionPolicy}) {
  return {
    type: 'CHOOSE_NAMESPACE',
    payload: {
      queryId,
      database,
      retentionPolicy,
    },
  }
}

export function chooseMeasurement(queryId, measurement) {
  return {
    type: 'CHOOSE_MEASUREMENT',
    payload: {
      queryId,
      measurement,
    },
  }
}

export function editRawText(queryId, rawText) {
  return {
    type: 'EDIT_RAW_TEXT',
    payload: {
      queryId,
      rawText,
    },
  }
}

export function setTimeRange(bounds) {
  return {
    type: 'SET_TIME_RANGE',
    payload: {
      bounds,
    },
  }
}

export function groupByTag(queryId, tagKey) {
  return {
    type: 'GROUP_BY_TAG',
    payload: {
      queryId,
      tagKey,
    },
  }
}

export function toggleTagAcceptance(queryId) {
  return {
    type: 'TOGGLE_TAG_ACCEPTANCE',
    payload: {
      queryId,
    },
  }
}

export function updateRawQuery(queryID, text) {
  return {
    type: 'UPDATE_RAW_QUERY',
    payload: {
      queryID,
      text,
    },
  }
}

export const updateQueryConfig = config => ({
  type: 'UPDATE_QUERY_CONFIG',
  payload: {
    config,
  },
})

export const editQueryStatus = (queryID, status) => ({
  type: 'EDIT_QUERY_STATUS',
  payload: {
    queryID,
    status,
  },
})

// Async actions
export const editRawTextAsync = (url, id, text) => async dispatch => {
  try {
    const {data} = await getQueryConfig(url, [{query: text, id}])
    const config = data.queries.find(q => q.id === id)
    dispatch(updateQueryConfig(config.queryConfig))
  } catch (error) {
    dispatch(errorThrown(error))
  }
}
