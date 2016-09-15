import u from 'updeep';

export default function time(state = {}, action) {
  switch (action.type) {
    case 'SET_TIME_BOUNDS': {
      const update = {
        bounds: action.payload.bounds,
        groupByInterval: action.payload.groupByInterval || state.groupByInterval,
      };

      return u(update, state);
    }
    case 'SET_AUTO_REFRESH': {
      const update = {
        autoRefresh: action.payload.milliseconds,
      };

      return u(update, state);
    }
    case 'SET_GROUP_BY': {
      return u({groupByInterval: action.payload.groupByInterval}, state);
    }
    default:
      return state;
  }
}
