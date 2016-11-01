export default function rules(state = {}, action) {
  switch (action.type) {
    case 'LOAD_RULE': {
      const {ruleID, rule} = action.payload;
      return Object.assign({}, state, {
        [ruleID]: rule,
      });
    }
  }
  return state;
}
