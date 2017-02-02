/**
 * Redux store enhancer (https://github.com/reactjs/redux/blob/master/docs/Glossary.md)
 * responsible for sending updates on data explorer state to a server to persist.
 * It subscribes a listener function to the store -- meaning every time the store emits an update
 * (after some state has changed), we'll have a chance to react.
 *
 * After the store emits an update, we'll queue a function to request a save in x number of
 * seconds.  The previous timer is cleared out as well, meaning we won't end up firing a ton of
 * saves in a short period of time.  Only after the store has been idle for x seconds will the save occur.
 */

const autoSaveTimer = (() => {
  let timer;

  return {
    set(cb) {
      const timeUntilSave = 300;
      timer = setTimeout(cb, timeUntilSave);
    },

    clear() {
      clearInterval(timer);
    },
  };
})();

export default function persistState() {
  return (next) => (reducer, initialState, enhancer) => {
    const store = next(reducer, initialState, enhancer);

    store.subscribe(() => {
      const state = {...store.getState()};
      const {panels, queryConfigs, timeRange} = state;

      window.localStorage.setItem('timeRange', JSON.stringify(timeRange));
      window.localStorage.setItem('panels', JSON.stringify(panels));
      window.localStorage.setItem('queryConfigs', JSON.stringify(queryConfigs));

      autoSaveTimer.clear();
      autoSaveTimer.set(() => {
        try {
          // save to localStorage
        } catch (err) {
          // console.error('Unable to save data explorer session: ', JSON.parse(response).error); // eslint-disable-line no-console
        }
      });
    });

    return store;
  };
}
