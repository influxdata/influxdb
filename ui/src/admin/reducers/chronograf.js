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

    case 'CHRONOGRAF_UPDATE_USER': {
      const {user, updatedUser} = action.payload
      return {
        ...state,
        users: state.users.map(
          u => (u.links.self === user.links.self ? {...updatedUser} : u)
        ),
      }
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
        // stale user does not necessarily have links, so uniqueness is on name,
        // provider, & scheme, except for a created users that is a duplicate
        // of an existing user, in which case a temp uuid is used to match
        users: state.users.filter(
          u => (user._tempID ? u._tempID !== user._tempID : u.id !== user.id)
        ),
      }
    }

    case 'CHRONOGRAF_ADD_ORGANIZATION': {
      const {organization} = action.payload
      return {...state, organizations: [organization, ...state.organizations]}
    }

    case 'CHRONOGRAF_RENAME_ORGANIZATION': {
      const {organization, newName} = action.payload
      return {
        ...state,
        organizations: state.organizations.map(
          o =>
            o.links.self === organization.links.self ? {...o, name: newName} : o
        ),
      }
    }

    case 'CHRONOGRAF_SYNC_ORGANIZATION': {
      const {staleOrganization, syncedOrganization} = action.payload
      return {
        ...state,
        organizations: state.organizations.map(
          o => (o.name === staleOrganization.name ? {...syncedOrganization} : o)
        ),
      }
    }

    case 'CHRONOGRAF_REMOVE_ORGANIZATION': {
      const {organization} = action.payload
      return {
        ...state,
        organizations: state.organizations.filter(
          o =>
            organization._tempID
              ? o._tempID !== organization._tempID
              : o.id !== organization.id
        ),
      }
    }
  }

  return state
}

export default adminChronograf
