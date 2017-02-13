export const loadLocalStorage = () => {
  try {
    const serializedState = localStorage.getItem('state');
    const parsedState = JSON.parse(serializedState) || {};

    return {...parsedState};
  } catch (err) {
    console.error(`Loading persisted state failed: ${err}`); // eslint-disable-line no-console
    return {};
  }
};

export const saveToLocalStorage = ({queryConfigs, timeRange}) => {
  try {
    window.localStorage.setItem('state', JSON.stringify({
      queryConfigs,
      timeRange,
    }));
  } catch (err) {
    console.error('Unable to save data explorer: ', JSON.parse(err)); // eslint-disable-line no-console
  }
};
