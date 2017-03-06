import {
  getUsers,
  getRoles,
  deleteRole as deleteRoleAJAX,
  deleteUser as deleteUserAJAX,
} from 'src/admin/apis'
import {killQuery as killQueryProxy} from 'shared/apis/metaQuery'

export const loadUsers = ({users}) => ({
  type: 'LOAD_USERS',
  payload: {
    users,
  },
})

export const killQuery = (queryID) => ({
  type: 'KILL_QUERY',
  payload: {
    queryID,
  },
})

export const setQueryToKill = (queryIDToKill) => ({
  type: 'SET_QUERY_TO_KILL',
  payload: {
    queryIDToKill,
  },
})

export const loadQueries = (queries) => ({
  type: 'LOAD_QUERIES',
  payload: {
    queries,
  },
})

export const loadRoles = ({roles}) => ({
  type: 'LOAD_ROLES',
  payload: {
    roles,
  },
})

export const deleteRole = (role) => ({
  type: 'DELETE_ROLE',
  payload: {
    role,
  },
})

export const deleteUser = (user) => ({
  type: 'DELETE_USER',
  payload: {
    user,
  },
})

// async actions
export const loadUsersAsync = (url) => async (dispatch) => {
  const {data} = await getUsers(url)
  dispatch(loadUsers(data))
}

export const loadRolesAsync = (url) => async (dispatch) => {
  const {data} = await getRoles(url)
  dispatch(loadRoles(data))
}

export const killQueryAsync = (source, queryID) => (dispatch) => {
  // optimistic update
  dispatch(killQuery(queryID))
  dispatch(setQueryToKill(null))

  // kill query on server
  killQueryProxy(source, queryID)
}

export const deleteRoleAsync = (role) => (dispatch) => {
  // optimistic update
  dispatch(deleteRole(role))

  // delete role on server
  deleteRoleAJAX(role.links.self)
}

export const deleteUserAsync = (role) => (dispatch) => {
  // optimistic update
  dispatch(deleteUser(role))

  // delete role on server
  deleteUserAJAX(role.links.self)
}
