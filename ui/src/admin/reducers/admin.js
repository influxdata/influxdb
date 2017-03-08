import reject from 'lodash/reject'

const newDefaultUser = {
  name: '',
  password: '',
  roles: [],
  permissions: [],
  links: {self: ''},
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
      const newUser = Object.assign({...newDefaultUser}, {isNew: true, isEditing: true})
      const newState = Object.assign({}, state, {
        users: [
          {...newUser},
          ...state.users,
        ],
      })
      return newState
    }

    case 'UPDATE_EDITING_USER': {
      const newState = {...state}
      const newUserState = action.payload.editingUser === null ? {...newDefaultUser} : {...action.payload}

      Object.assign(newState.ephemeral, newUserState)
      return newState
    }

    case 'CLEAR_EDITING_MODE': {
      const newState = {...state}
      newState.users.forEach((user) => user.isEditing = false)
      Object.assign(newState.ephemeral.editingUser, newDefaultUser)
      return newState
    }

    // case 'CREATE_USER': {
    //   const {user, source} = action.payload
    //   const userLink = `${this.props.source.links.users}/${user.name}`
    //   Object.assign(user, {links: {self: userLink}})
    // }

    case 'REMOVE_ADDED_USER': {
      const newUsers = [...state.users]

      if (action.payload.user) {
       // find and remove first user in list with name
        const i = newUsers.findIndex((user) => user.name === action.payload.user.name)
        newUsers.splice(i, 1)
      } else {
        newUsers.shift()
      }

      const newState = Object.assign({}, state, {
        users: newUsers,
      })
      return newState
    }

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
