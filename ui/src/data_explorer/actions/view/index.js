import uuid from 'node-uuid'

import {getQueryConfig} from 'shared/apis'

import {errorThrown} from 'shared/actions/errors'

export const addQuery = () => ({
  type: 'DE_ADD_QUERY',
  payload: {
    queryID: uuid.v4(),
  },
})

export const deleteQuery = queryID => ({
  type: 'DE_DELETE_QUERY',
  payload: {
    queryID,
  },
})

export const toggleField = (queryId, fieldFunc) => ({
  type: 'DE_TOGGLE_FIELD',
  payload: {
    queryId,
    fieldFunc,
  },
})

export const groupByTime = (queryId, time) => ({
  type: 'DE_GROUP_BY_TIME',
  payload: {
    queryId,
    time,
  },
})

export const fill = (queryId, value) => ({
  type: 'DE_FILL',
  payload: {
    queryId,
    value,
  },
})

export const removeFuncs = (queryID, fields, groupBy) => ({
  type: 'DE_REMOVE_FUNCS',
  payload: {
    queryID,
    fields,
    groupBy,
  },
})

export const applyFuncsToField = (queryId, fieldFunc, groupBy) => ({
  type: 'DE_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryId,
    fieldFunc,
    groupBy,
  },
})

export const chooseTag = (queryId, tag) => ({
  type: 'DE_CHOOSE_TAG',
  payload: {
    queryId,
    tag,
  },
})

export const chooseNamespace = (queryId, {database, retentionPolicy}) => ({
  type: 'DE_CHOOSE_NAMESPACE',
  payload: {
    queryId,
    database,
    retentionPolicy,
  },
})

export const chooseMeasurement = (queryId, measurement) => ({
  type: 'DE_CHOOSE_MEASUREMENT',
  payload: {
    queryId,
    measurement,
  },
})

export const editRawText = (queryId, rawText) => ({
  type: 'DE_EDIT_RAW_TEXT',
  payload: {
    queryId,
    rawText,
  },
})

export const setTimeRange = bounds => ({
  type: 'DE_SET_TIME_RANGE',
  payload: {
    bounds,
  },
})

export const groupByTag = (queryId, tagKey) => ({
  type: 'DE_GROUP_BY_TAG',
  payload: {
    queryId,
    tagKey,
  },
})

export const toggleTagAcceptance = queryId => ({
  type: 'DE_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryId,
  },
})

export const updateRawQuery = (queryID, text) => ({
  type: 'DE_UPDATE_RAW_QUERY',
  payload: {
    queryID,
    text,
  },
})

export const updateQueryConfig = config => ({
  type: 'DE_UPDATE_QUERY_CONFIG',
  payload: {
    config,
  },
})

export const addInitialField = (queryID, field, groupBy) => ({
  type: 'DE_ADD_INITIAL_FIELD',
  payload: {
    queryID,
    field,
    groupBy,
  },
})

export const editQueryStatus = (queryID, status) => ({
  type: 'DE_EDIT_QUERY_STATUS',
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
