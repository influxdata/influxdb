import {defaultRuleConfigs} from 'src/kapacitor/constants';

export default function rules(state = {}, action) {
  switch (action.type) {
    case 'LOAD_DEFAULT_RULE': {
      const {queryID, ruleID} = action.payload;
      return Object.assign({}, state, {
        [ruleID]: {
          id: ruleID,
          queryID,
          trigger: 'threshold',
          values: defaultRuleConfigs.threshold,
        },
      });
    }

    case 'LOAD_RULE': {
      const {ruleID, rule} = action.payload;
      return Object.assign({}, state, {
        [ruleID]: rule,
      });
    }

    case 'CHOOSE_TRIGGER': {
      const trigger = action.payload.trigger;
      const ruleID = action.payload.ruleID;
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          trigger: trigger.toLowerCase(),
          values: defaultRuleConfigs[trigger.toLowerCase()],
        }),
      });
    }

    case 'UPDATE_RULE_VALUES': {
      const {ruleID, trigger, values} = action.payload;
      return Object.assign({}, state, {
        [ruleID]: Object.assign({}, state[ruleID], {
          trigger: trigger.toLowerCase(),
          values,
        }),
      });
    }
  }
  return state;
}
