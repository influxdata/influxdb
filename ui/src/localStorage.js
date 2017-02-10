import _ from 'lodash';

export const loadLocalStorage = () => {
  try {
    const serializedState = localStorage.getItem('state');
    const defaultTimeRange = {upper: null, lower: 'now() - 15m'};

    if (serializedState === null) {
      return {timeRange: defaultTimeRange};
    }

    const parsedState = JSON.parse(serializedState) || {};
    const timeRange = _.isEmpty(parsedState.timeRange) ? defaultTimeRange : parsedState.timeRange;

    return {...parsedState, timeRange};
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
