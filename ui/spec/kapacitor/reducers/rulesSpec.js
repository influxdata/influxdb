import reducer from 'src/kapacitor/reducers/rules';
import {defaultRuleConfigs} from 'src/kapacitor/constants';

import {
  chooseTrigger,
  updateRuleValues,
  updateMessage,
  updateAlerts,
} from 'src/kapacitor/actions/view';

describe('Kapacitor.Reducers.rules', () => {
  it('can choose a trigger', () => {
      const ruleID = 1;
      const initialState = {
        [ruleID]: {
          id: ruleID,
          queryID: 988,
          trigger: '',
        }
      };

    let newState = reducer(initialState, chooseTrigger(ruleID, 'deadman'));
    expect(newState[ruleID].trigger).to.equal('deadman');
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.deadman);

    newState = reducer(initialState, chooseTrigger(ruleID, 'relative'));
    expect(newState[ruleID].trigger).to.equal('relative');
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.relative);

    newState = reducer(initialState, chooseTrigger(ruleID, 'threshold'));
    expect(newState[ruleID].trigger).to.equal('threshold');
    expect(newState[ruleID].values).to.equal(defaultRuleConfigs.threshold);
  });

  it('can update the values', () => {
    const ruleID = 1;
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        trigger: 'deadman',
        values: defaultRuleConfigs.deadman
      }
    };

    const newDeadmanValues = {duration: '5m'};
    const newState = reducer(initialState, updateRuleValues(ruleID, 'deadman', newDeadmanValues));
    expect(newState[ruleID].values).to.equal(newDeadmanValues);

    const newRelativeValues = {func: 'max', change: 'change'};
    const finalState = reducer(newState, updateRuleValues(ruleID, 'relative', newRelativeValues));
    expect(finalState[ruleID].trigger).to.equal('relative');
    expect(finalState[ruleID].values).to.equal(newRelativeValues);
  });

  it('can update the message', () => {
    const ruleID = 1;
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        message: '',
      }
    };

    const message = 'im a kapacitor rule message';
    const newState = reducer(initialState, updateMessage(ruleID, message));
    expect(newState[ruleID].message).to.equal(message);
  });

  it('can update the alerts', () => {
    const ruleID = 1;
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        alerts: [],
      }
    };

    const alerts = ['slack'];
    const newState = reducer(initialState, updateAlerts(ruleID, alerts));
    expect(newState[ruleID].alerts).to.equal(alerts);
  });
});
