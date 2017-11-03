import {isSameUser} from 'shared/reducers/helpers/auth'

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
          // stale user does not have links, so uniqueness is on name, provider, & scheme
          u => (isSameUser(u, staleUser) ? {...syncedUser} : u)
        ),
      }
    }

    case 'CHRONOGRAF_REMOVE_USER': {
      const {user} = action.payload
      return {
        ...state,
        // stale user does not have links, so uniqueness is on name, provider, & scheme
        users: state.users.filter(u => !isSameUser(u, user)),
      }
    }
  }

  return state
}

export default adminChronograf
