import uuid from 'node-uuid';

export function fetchRule(ruleID) {
  return (dispatch) => {
    // do some ajax
    const queryID = uuid.v4();
    dispatch({
      type: 'LOAD_RULE',
      payload: {
        ruleID,
        rule: {
          queryID,
          otherProperties: {},
        },
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
