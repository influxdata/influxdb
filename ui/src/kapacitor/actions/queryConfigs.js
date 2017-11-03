export const chooseNamespace = (queryID, {database, retentionPolicy}) => ({
  type: 'KAPA_CHOOSE_NAMESPACE',
  payload: {
    queryID,
    database,
    retentionPolicy,
  },
})

export const chooseMeasurement = (queryID, measurement) => ({
  type: 'KAPA_CHOOSE_MEASUREMENT',
  payload: {
    queryID,
    measurement,
  },
})

export const chooseTag = (queryID, tag) => ({
  type: 'KAPA_CHOOSE_TAG',
  payload: {
    queryID,
    tag,
  },
})

export const groupByTag = (queryID, tagKey) => ({
  type: 'KAPA_GROUP_BY_TAG',
  payload: {
    queryID,
    tagKey,
  },
})

export const toggleTagAcceptance = queryID => ({
  type: 'KAPA_TOGGLE_TAG_ACCEPTANCE',
  payload: {
    queryID,
  },
})

export const toggleField = (queryID, fieldFunc) => ({
  type: 'KAPA_TOGGLE_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

export const applyFuncsToField = (queryID, fieldFunc) => ({
  type: 'KAPA_APPLY_FUNCS_TO_FIELD',
  payload: {
    queryID,
    fieldFunc,
  },
})

export const groupByTime = (queryID, time) => ({
  type: 'KAPA_GROUP_BY_TIME',
  payload: {
    queryID,
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
