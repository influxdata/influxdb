import uuid from 'node-uuid';

export function fetchRule() { // ruleID
  return (dispatch) => {
    // do some ajax to get the rule. put it in the payload
    const queryID = uuid.v4();
    dispatch({
      type: 'LOAD_RULE',
      payload: {},
    });

    dispatch({
      type: 'ADD_KAPACITOR_QUERY',
      payload: {
        queryId: queryID,
      },
    });
  };
}

export function loadDefaultRule() {
  return (dispatch) => {
    const queryID = uuid.v4();
    const ruleID = uuid.v4();
    dispatch({
      type: 'LOAD_DEFAULT_RULE',
      payload: {
        queryID,
        ruleID,
      },
    });
    dispatch({
      type: 'ADD_KAPACITOR_QUERY',
      payload: {
        queryId: queryID,
      },
    });
  };
}

export function chooseTrigger(ruleID, trigger) {
  return {
    type: 'CHOOSE_TRIGGER',
    payload: {
      ruleID,
      trigger,
    },
  };
}

export function updateRuleValues(ruleID, trigger, values) {
  return {
    type: 'UPDATE_RULE_VALUES',
    payload: {
      ruleID,
      trigger,
      values,
    },
  };
}
