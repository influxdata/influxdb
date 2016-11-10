import reducer from 'src/kapacitor/reducers/rules';
import {defaultRuleConfigs} from 'src/kapacitor/constants';

import {
  chooseTrigger,
  updateRuleValues,
  updateMessage,
  updateAlerts,
  updateRuleName,
  deleteRule,
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

  it('can update the name', () => {
    const ruleID = 1;
    const name = 'New name'
    const initialState = {
      [ruleID]: {
        id: ruleID,
        queryID: 988,
        name: 'Random album title',
      }
    };

    const newState = reducer(initialState, updateRuleName(ruleID, name));
    expect(newState[ruleID].name).to.equal(name);
  });

  it('it can delete a rule', () => {
    const rule1 = 1;
    const rule2 = 2;
    const initialState = {
      [rule1]: {
        id: rule1,
      },
      [rule2]: {
        id: rule2,
      },
    };

    expect(Object.keys(initialState).length).to.equal(2);
    const newState = reducer(initialState, deleteRule(rule2));
    expect(Object.keys(newState).length).to.equal(1);
    expect(newState[rule1]).to.equal(initialState[rule1]);
  });
});
