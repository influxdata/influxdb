import reject from 'lodash/reject'

const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
  isNew: true,
}

const initialState = {
  users: [],
  roles: [],
  ephemeral: {
    editingUser: {},
  },
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

    case 'SET_EDITING_MODE': {
      const newState = {...state}
      newState.ephemeral.isEditing = action.payload.isEditing
      return newState
    }

    case 'RESET_EDITING_USER': {
      return {...state, ephemeral: {...state.ephemeral, editingUser: {...newDefaultUser}}}
    }

    case 'EDIT_USER': {
      const {user, updates} = action.payload
      const newState = {
        users: state.users.map(u => {
          const output = u.name === user.name ? {...u, ...updates} : u
          return output
        }),
      }
      return {...state, ...newState}
    }

    case 'CLEAR_EDITING_MODE': {
      const newState = {
        users: state.users.map(u => {
          u.isEditing = false
          return u
        }),
      }
      return {...state, ...newState}
    }

    // case 'CREATE_USER': {
    //   const {user, source} = action.payload
    //   const userLink = `${this.props.source.links.users}/${user.name}`
    //   Object.assign(user, {links: {self: userLink}})
    // }

    case 'DELETE_ROLE': {
      const {role} = action.payload
      const newState = {
        roles: state.roles.filter(r => r.name !== role.name),
      }

      return {...state, ...newState}
    }

    case 'DELETE_USER': {
      const {user} = action.payload
      const newState = {
        users: state.users.filter(u => u.name !== user.name),
      }

      return {...state, ...newState}
    }

    case 'LOAD_QUERIES': {
      return {...state, ...action.payload}
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
