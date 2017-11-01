const initialState = {
  users: [],
  organizations: [],
}

const adminChronograf = (state = initialState, action) => {
  switch (action.type) {
    case 'CHRONOGRAF_LOAD_USERS': {
      return {...state, ...action.payload}
    }
  }

  return state
}

export default adminChronograf
