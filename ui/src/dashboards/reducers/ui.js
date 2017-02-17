const initialState = {
  dashboards: [],
};

export default function ui(state = initialState, action) {
  switch (action.type) {
    case 'LOAD_DASHBOARDS': {
      const {dashboards} = action.payload;
      const newState = {
        dashboards,
      };

      return {...state, ...newState};
    }
  }

  return state;
}
