import {saveExplorer} from 'src/data_explorer/api';

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
      const timeUntilSave = 3000;
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
      const state = Object.assign({}, store.getState());
      const explorerID = state.activeExplorer.id;
      const name = state.activeExplorer.name;
      if (!explorerID) {
        return;
      }
      const {panels, queryConfigs} = state;
      autoSaveTimer.clear();
      autoSaveTimer.set(() => {
        saveExplorer({panels, queryConfigs, explorerID, name}).then((_) => {
          // TODO: This is a no-op currently because we don't have any feedback in the UI around saving, but maybe we do something in the future?
          // If we ever show feedback in the UI, we could potentially indicate to remove it here.
        }).catch(({response}) => {
          console.error('Unable to save data explorer session: ', JSON.parse(response).error); // eslint-disable-line no-console
        });
      });
    });

    return store;
  };
}
