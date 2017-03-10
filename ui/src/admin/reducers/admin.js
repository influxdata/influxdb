import reject from 'lodash/reject'

const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
  links: {self: ''},
  isNew: true,
}
const newDefaultRole = {
  name: '',
  permissions: [],
  users: [],
  links: {self: ''},
  isNew: true,
}

const initialState = {
  users: [],
  roles: [],
  permissions: [],
  queries: [],
  queryIDToKill: null,
}

export default function admin(state = initialState, action) {
  switch (action.type) {
    case 'LOAD_USERS': {
      return {...state, ...action.payload}
    }

    case 'LOAD_ROLES': {
      return {...state, ...action.payload}
    }

    case 'LOAD_PERMISSIONS': {
      return {...state, ...action.payload}
    }

    case 'ADD_USER': {
      const newUser = {...newDefaultUser, isEditing: true}
      return {
        ...state,
        users: [
          newUser,
          ...state.users,
        ],
      }
    }

    case 'ADD_ROLE': {
      const newRole = {...newDefaultRole, isEditing: true}
      return {
        ...state,
        roles: [
          newRole,
          ...state.roles,
        ],
      }
    }

    case 'CREATE_USER_SUCCESS': {
      const {user, createdUser} = action.payload
      const newState = {
        users: state.users.map(u => u.links.self === user.links.self ? {...createdUser} : u),
      }
      return {...state, ...newState}
    }

    case 'CREATE_ROLE_SUCCESS': {
      const {role, createdRole} = action.payload
      const newState = {
        roles: state.roles.map(r => r.links.self === role.links.self ? {...createdRole} : r),
      }
      return {...state, ...newState}
    }

    case 'EDIT_USER': {
      const {user, updates} = action.payload
      const newState = {
        users: state.users.map(u => u.links.self === user.links.self ? {...u, ...updates} : u),
      }
      return {...state, ...newState}
    }

    case 'EDIT_ROLE': {
      const {role, updates} = action.payload
      const newState = {
        roles: state.roles.map(r => r.links.self === role.links.self ? {...r, ...updates} : r),
      }
      return {...state, ...newState}
    }

    case 'DELETE_USER': {
      const {user} = action.payload
      const newState = {
        users: state.users.filter(u => u.links.self !== user.links.self),
      }

      return {...state, ...newState}
    }

    case 'DELETE_ROLE': {
      const {role} = action.payload
      const newState = {
        roles: state.roles.filter(r => r.links.self !== role.links.self),
      }

      return {...state, ...newState}
    }

    case 'LOAD_QUERIES': {
      return {...state, ...action.payload}
    }

    case 'FILTER_USERS': {
      const {text} = action.payload
      const newState = {
        users: state.users.map(u => {
          u.hidden = !u.name.toLowerCase().includes(text)
          return u
        }),
      }
      return {...state, ...newState}
    }

    case 'FILTER_ROLES': {
      const {text} = action.payload
      const newState = {
        roles: state.roles.map(r => {
          r.hidden = !r.name.toLowerCase().includes(text)
          return r
        }),
      }
      return {...state, ...newState}
    }

    case 'KILL_QUERY': {
      const {queryID} = action.payload
      const nextState = {
        queries: reject(state.queries, (q) => +q.id === +queryID),
      }

      return {...state, ...nextState}
    }

    case 'SET_QUERY_TO_KILL': {
      return {...state, ...action.payload}
    }
  }

  return state
}
