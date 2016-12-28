export const spyActions = (actions) => Object.keys(actions)
  .reduce((acc, a) => {
    acc[a] = (...evt) => {
      action(a)(...evt);
      return actions[a](...evt);
    };
    return acc;
  }, {});
