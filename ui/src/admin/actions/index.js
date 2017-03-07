import {
  getUsers as getUsersAPI,
  getRoles as getRolesAPI,
  createUser as createUserAPI,
} from 'src/admin/apis'
import {killQuery as killQueryProxy} from 'shared/apis/metaQuery'
import {publishNotification} from 'src/shared/actions/notifications';

export const loadUsers = ({users}) => ({
  type: 'LOAD_USERS',
  payload: {
    users,
  },
})

export const loadRoles = ({roles}) => ({
  type: 'LOAD_ROLES',
  payload: {
    roles,
  },
})

export const addUser = (user) => ({
  type: 'ADD_USER',
  payload: {
    user,
  },
})

export const errorAddUser = (user) => ({
  type: 'ERROR_ADD_USER',
  payload: {
    user,
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

// async actions
export const loadUsersAsync = (url) => async (dispatch) => {
  const {data} = await getUsersAPI(url)
  dispatch(loadUsers(data))
}

export const loadRolesAsync = (url) => async (dispatch) => {
  const {data} = await getRolesAPI(url)
  dispatch(loadRoles(data))
}

export const addUserAsync = (url, user) => async (dispatch) => {
  dispatch(addUser(user))

  try {
    await createUserAPI(url, user)
    dispatch(publishNotification('success', 'User created successfully'))
  } catch (error) {
    dispatch(publishNotification('error', 'Failed to create user'))
    setTimeout(() => dispatch(errorAddUser(user)), 1500)
  }
}

export const killQueryAsync = (source, queryID) => (dispatch) => {
  // optimistic update
  dispatch(killQuery(queryID))
  dispatch(setQueryToKill(null))

  // kill query on server
  killQueryProxy(source, queryID)
}
