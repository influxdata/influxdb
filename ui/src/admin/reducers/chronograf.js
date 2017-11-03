const initialState = {
  users: [],
  organizations: [],
}

const adminChronograf = (state = initialState, action) => {
  switch (action.type) {
    case 'CHRONOGRAF_LOAD_USERS': {
      return {...state, ...action.payload}
    }

    case 'CHRONOGRAF_LOAD_ORGANIZATIONS': {
      return {...state, ...action.payload}
    }

    case 'CHRONOGRAF_ADD_USER': {
      const {user} = action.payload
      return {...state, users: [user, ...state.users]}
    }

    case 'CHRONOGRAF_SYNC_USER': {
      const {staleUser, syncedUser} = action.payload
      return {
        ...state,
        users: state.users.map(
          u =>
            // stale user does not have links, so uniqueness is on name, provider, & scheme
            u.name === staleUser.name &&
            u.provider === staleUser.provider &&
            u.scheme === staleUser.scheme
              ? {...syncedUser}
              : u
        ),
      }
    }

    case 'CHRONOGRAF_DELETE_USER': {
      const {user} = action.payload
      return {
        ...state,
        users: state.users.filter(u => u.links.self !== user.links.self),
      }
    }
  }

  return state
}

export default adminChronograf
