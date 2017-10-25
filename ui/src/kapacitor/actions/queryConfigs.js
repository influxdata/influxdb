export const chooseNamespace = (queryId, {database, retentionPolicy}) => ({
  type: 'KAPA_CHOOSE_NAMESPACE',
  payload: {
    queryId,
    database,
    retentionPolicy,
  },
})

export const chooseMeasurement = (queryId, measurement) => ({
  type: 'KAPA_CHOOSE_MEASUREMENT',
  payload: {
    queryId,
    measurement,
  },
})

export const chooseTag = (queryId, tag) => ({
  type: 'KAPA_CHOOSE_TAG',
  payload: {
    queryId,
    tag,
  },
})

export const groupByTag = (queryId, tagKey) => ({
  type: 'KAPA_GROUP_BY_TAG',
  payload: {
    queryId,
    tagKey,
  },
})

export const toggleTagAcceptance = queryId => ({
  type: 'KAPA_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryId,
  },
})

export const toggleField = (queryId, fieldFunc) => ({
  type: 'KAPA_TOGGLE_FIELD',
  payload: {
    queryId,
    fieldFunc,
  },
})

export const applyFuncsToField = (queryId, fieldFunc) => ({
  type: 'KAPA_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryId,
    fieldFunc,
  },
})

export const groupByTime = (queryId, time) => ({
  type: 'KAPA_GROUP_BY_TIME',
  payload: {
    queryId,
    time,
  },
})

export const removeFuncs = (queryID, fields) => ({
  type: 'KAPA_REMOVE_FUNCS',
  payload: {
    queryID,
    fields,
  },
})
