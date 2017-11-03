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

export const toggleField = (queryID, fieldFunc) => ({
  type: 'DE_TOGGLE_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

export const groupByTime = (queryID, time) => ({
  type: 'DE_GROUP_BY_TIME',
  payload: {
    queryID,
    time,
  },
})

export const fill = (queryID, value) => ({
  type: 'DE_FILL',
  payload: {
    queryID,
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

export const applyFuncsToField = (queryID, fieldFunc, groupBy) => ({
  type: 'DE_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryID,
    fieldFunc,
    groupBy,
  },
})

export const chooseTag = (queryID, tag) => ({
  type: 'DE_CHOOSE_TAG',
  payload: {
    queryID,
    tag,
  },
})

export const chooseNamespace = (queryID, {database, retentionPolicy}) => ({
  type: 'DE_CHOOSE_NAMESPACE',
  payload: {
    queryID,
    database,
    retentionPolicy,
  },
})

export const chooseMeasurement = (queryID, measurement) => ({
  type: 'DE_CHOOSE_MEASUREMENT',
  payload: {
    queryID,
    measurement,
  },
})

export const editRawText = (queryID, rawText) => ({
  type: 'DE_EDIT_RAW_TEXT',
  payload: {
    queryID,
    rawText,
  },
})

export const setTimeRange = bounds => ({
  type: 'DE_SET_TIME_RANGE',
  payload: {
    bounds,
  },
})

export const groupByTag = (queryID, tagKey) => ({
  type: 'DE_GROUP_BY_TAG',
  payload: {
    queryID,
    tagKey,
  },
})

export const toggleTagAcceptance = queryID => ({
  type: 'DE_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryID,
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
