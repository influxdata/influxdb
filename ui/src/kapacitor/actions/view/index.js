import defaultQueryConfig from 'src/utils/defaultQueryConfig';

export function fetchTask(taskID) {
  // do some ajax
  //
  return {
    type: 'K_LOAD_TASK',
    payload: {
      taskID,
      task: {
        query: defaultQueryConfig(''),
        otherProperties: {},
      },
    },
  };
}

export function toggleField(taskId, fieldFunc) {
  return {
    type: 'K_TOGGLE_FIELD',
    payload: {
      queryId,
      fieldFunc,
    },
  };
}

export function groupByTime(queryId, time) {
  return {
    type: 'K_GROUP_BY_TIME',
    payload: {
      queryId,
      time,
    },
  };
}

export function applyFuncsToField(queryId, fieldFunc) {
  return {
    type: 'K_APPLY_FUNCS_TO_FIELD',
    payload: {
      queryId,
      fieldFunc,
    },
  };
}

export function chooseTag(queryId, tag) {
  return {
    type: 'K_CHOOSE_TAG',
    payload: {
      queryId,
      tag,
    },
  };
}

export function chooseNamespace(queryId, {database, retentionPolicy}) {
  return {
    type: 'K_CHOOSE_NAMESPACE',
    payload: {
      queryId,
      database,
      retentionPolicy,
    },
  };
}

export function chooseMeasurement(queryId, measurement) {
  return {
    type: 'K_CHOOSE_MEASUREMENT',
    payload: {
      queryId,
      measurement,
    },
  };
}

export function groupByTag(queryId, tagKey) {
  return {
    type: 'K_GROUP_BY_TAG',
    payload: {
      queryId,
      tagKey,
    },
  };
}

export function toggleTagAcceptance(queryId) {
  return {
    type: 'K_TOGGLE_TAG_ACCEPTANCE',
    payload: {
      queryId,
    },
  };
}
