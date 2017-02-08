import uuid from 'node-uuid';

export function createPanel() {
  return {
    type: 'CREATE_PANEL',
    payload: {
      panelID: uuid.v4(), // for the default Panel
      queryID: uuid.v4(), // for the default Query
    },
  };
}

export function renamePanel(panelId, name) {
  return {
    type: 'RENAME_PANEL',
    payload: {
      panelId,
      name,
    },
  };
}

export function deletePanel(panelId) {
  return {
    type: 'DELETE_PANEL',
    payload: {
      panelId,
    },
  };
}

export function addQuery(panelId, options) {
  return {
    type: 'ADD_QUERY',
    payload: {
      panelId,
      queryId: uuid.v4(),
      options,
    },
  };
}

export function deleteQuery(panelId, queryId) {
  return {
    type: 'DELETE_QUERY',
    payload: {
      queryId,
      panelId,
    },
  };
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
  };
}

export function groupByTime(queryId, time) {
  return {
    type: 'GROUP_BY_TIME',
    payload: {
      queryId,
      time,
    },
  };
}

export function applyFuncsToField(queryId, fieldFunc) {
  return {
    type: 'APPLY_FUNCS_TO_FIELD',
    payload: {
      queryId,
      fieldFunc,
    },
  };
}

export function chooseTag(queryId, tag) {
  return {
    type: 'CHOOSE_TAG',
    payload: {
      queryId,
      tag,
    },
  };
}

export function chooseNamespace(queryId, {database, retentionPolicy}) {
  return {
    type: 'CHOOSE_NAMESPACE',
    payload: {
      queryId,
      database,
      retentionPolicy,
    },
  };
}

export function chooseMeasurement(queryId, measurement) {
  return {
    type: 'CHOOSE_MEASUREMENT',
    payload: {
      queryId,
      measurement,
    },
  };
}

export function editRawText(queryId, rawText) {
  return {
    type: 'EDIT_RAW_TEXT',
    payload: {
      queryId,
      rawText,
    },
  };
}

export function setTimeRange(range) {
  return {
    type: 'SET_TIME_RANGE',
    payload: range,
  };
}

export function groupByTag(queryId, tagKey) {
  return {
    type: 'GROUP_BY_TAG',
    payload: {
      queryId,
      tagKey,
    },
  };
}

export function toggleTagAcceptance(queryId) {
  return {
    type: 'TOGGLE_TAG_ACCEPTANCE',
    payload: {
      queryId,
    },
  };
}

export function updateRawQuery(queryID, text) {
  return {
    type: 'UPDATE_RAW_QUERY',
    payload: {
      queryID,
      text,
    },
  };
}

export function activatePanel(panelID) {
  return {
    type: 'ACTIVATE_PANEL',
    payload: {
      panelID,
    },
  };
}
