const initialState = {
  upper: null,
  lower: 'now() - 15m',
};

export default function timeRange(state = initialState, action) {
  switch (action.type) {
    case 'SET_TIME_RANGE': {
      const {upper, lower} = action.payload;
      const newState = {
        upper,
        lower,
      };

      return {...state, ...newState};
    }
  }
  return state;
}
