import update from 'react-addons-update';

const initialState = {
  upper: null,
  lower: 'now() - 15m',
};

export default function timeRange(state = initialState, action) {
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
