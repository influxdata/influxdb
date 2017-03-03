export const loadLocalStorage = () => {
  try {
    const serializedState = localStorage.getItem('state');

    return JSON.parse(serializedState) || {};
  } catch (err) {
    console.error(`Loading persisted state failed: ${err}`); // eslint-disable-line no-console
    return {};
  }
};

export const saveToLocalStorage = ({app: {persisted}, queryConfigs, timeRange, dataExplorer}) => {
  try {
    const appPersisted = Object.assign({}, {app: {persisted}})

    window.localStorage.setItem('state', JSON.stringify({
      ...appPersisted,
      queryConfigs,
      timeRange,
      dataExplorer,
    }));
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err)); // eslint-disable-line no-console
  }
};
