import update from 'react-addons-update';

export default function timeRange(state = {}, action) {
  switch (action.type) {
    case 'SET_TIME_RANGE': {
      const {upper, lower} = action.payload;

      return update(state, {
        ['lower']: {$set: lower},
        ['upper']: {$set: upper},
      });
    }
  }
  return state;
}
