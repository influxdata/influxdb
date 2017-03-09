import {
  getUsers as getUsersAJAX,
  getRoles as getRolesAJAX,
  createUser as createUserAJAX,
  deleteRole as deleteRoleAJAX,
  deleteUser as deleteUserAJAX,
  updateRoleUsers as updateRoleUsersAJAX,
  updateRolePermissions as updateRolePermissionsAJAX,
} from 'src/admin/apis'
import {killQuery as killQueryProxy} from 'shared/apis/metaQuery'
import {publishNotification} from 'src/shared/actions/notifications';


import {publishNotification} from 'src/shared/actions/notifications';

import {ADMIN_NOTIFICATION_DELAY} from 'shared/constants'

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

export const addUser = () => ({
  type: 'ADD_USER',
})

export const createUserSuccess = (user, createdUser) => ({
  type: 'CREATE_USER_SUCCESS',
  payload: {
    user,
    createdUser,
  },
})

export const editUser = (user, updates) => ({
  type: 'EDIT_USER',
  payload: {
    user,
    updates,
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

export const filterRoles = (text) => ({
  type: 'FILTER_ROLES',
  payload: {
    text,
  },
})

export const filterUsers = (text) => ({
  type: 'FILTER_USERS',
  payload: {
    text,
  },
})

// async actions
export const loadUsersAsync = (url) => async (dispatch) => {
  const {data} = await getUsersAJAX(url)
  dispatch(loadUsers(data))
}

export const loadRolesAsync = (url) => async (dispatch) => {
  const {data} = await getRolesAJAX(url)
  dispatch(loadRoles(data))
}

export const createUserAsync = (url, user) => async (dispatch) => {
  try {
    const {data} = await createUserAJAX(url, user)
    dispatch(publishNotification('success', 'User created successfully'))
    dispatch(createUserSuccess(user, data))
  } catch (error) {
    // undo optimistic update
    dispatch(publishNotification('error', `Failed to create user: ${error.data.message}`))
    setTimeout(() => dispatch(deleteUser(user)), ADMIN_NOTIFICATION_DELAY)
  }
}

export const killQueryAsync = (source, queryID) => (dispatch) => {
  // optimistic update
  dispatch(killQuery(queryID))
  dispatch(setQueryToKill(null))

  // kill query on server
  killQueryProxy(source, queryID)
}

export const deleteRoleAsync = (role, addFlashMessage) => (dispatch) => {
  // optimistic update
  dispatch(deleteRole(role))

  // delete role on server
  deleteRoleAJAX(role.links.self, addFlashMessage, role.name)
}

export const deleteUserAsync = (user, addFlashMessage) => (dispatch) => {
  // optimistic update
  dispatch(deleteUser(user))

  // delete user on server
  deleteUserAJAX(user.links.self, addFlashMessage, user.name)
}

export const updateRoleUsersAsync = (users, role) => async (dispatch) => {
  try {
    await updateRoleUsersAJAX(role.links.self, users)
    dispatch(publishNotification('success', 'Role users updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to update role: ${error.data.message}`))
  }
}

export const updateRolePermissionsAsync = (permissions, role) => async (dispatch) => {
  try {
    await updateRolePermissionsAJAX(role.links.self, permissions)
    dispatch(publishNotification('success', 'Role permissions updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to updated role:  ${error.data.message}`))
  }
}
