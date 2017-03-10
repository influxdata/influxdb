import {
  getUsers as getUsersAJAX,
  getRoles as getRolesAJAX,
  getPermissions as getPermissionsAJAX,
  createUser as createUserAJAX,
  createRole as createRoleAJAX,
  deleteUser as deleteUserAJAX,
  deleteRole as deleteRoleAJAX,
  updateRole as updateRoleAJAX,
  updateUser as updateUserAJAX,
} from 'src/admin/apis'

import {killQuery as killQueryProxy} from 'shared/apis/metaQuery'
import {publishNotification} from 'src/shared/actions/notifications';
import {ADMIN_NOTIFICATION_DELAY} from 'src/admin/constants'

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

export const loadPermissions = ({permissions}) => ({
  type: 'LOAD_PERMISSIONS',
  payload: {
    permissions,
  },
})

export const addUser = () => ({
  type: 'ADD_USER',
})

export const addRole = () => ({
  type: 'ADD_ROLE',
})

export const createUserSuccess = (user, createdUser) => ({
  type: 'CREATE_USER_SUCCESS',
  payload: {
    user,
    createdUser,
  },
})

export const createRoleSuccess = (role, createdRole) => ({
  type: 'CREATE_ROLE_SUCCESS',
  payload: {
    role,
    createdRole,
  },
})

export const editUser = (user, updates) => ({
  type: 'EDIT_USER',
  payload: {
    user,
    updates,
  },
})

export const editRole = (role, updates) => ({
  type: 'EDIT_ROLE',
  payload: {
    role,
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

export const deleteUser = (user) => ({
  type: 'DELETE_USER',
  payload: {
    user,
  },
})

export const deleteRole = (role) => ({
  type: 'DELETE_ROLE',
  payload: {
    role,
  },
})

export const filterUsers = (text) => ({
  type: 'FILTER_USERS',
  payload: {
    text,
  },
})

export const filterRoles = (text) => ({
  type: 'FILTER_ROLES',
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

export const loadPermissionsAsync = (url) => async (dispatch) => {
  const {data} = await getPermissionsAJAX(url)
  dispatch(loadPermissions(data))
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

export const createRoleAsync = (url, role) => async (dispatch) => {
  try {
    const {data} = await createRoleAJAX(url, role)
    dispatch(publishNotification('success', 'Role created successfully'))
    dispatch(createRoleSuccess(role, data))
  } catch (error) {
    // undo optimistic update
    dispatch(publishNotification('error', `Failed to create role: ${error.data.message}`))
    setTimeout(() => dispatch(deleteRole(role)), ADMIN_NOTIFICATION_DELAY)
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

export const updateRoleUsersAsync = (role, users) => async (dispatch) => {
  try {
    await updateRoleAJAX(role.links.self, users, role.permissions)
    dispatch(publishNotification('success', 'Role users updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to update role: ${error.data.message}`))
  }
}

export const updateRolePermissionsAsync = (role, permissions) => async (dispatch) => {
  try {
    await updateRoleAJAX(role.links.self, role.users, permissions)
    dispatch(publishNotification('success', 'Role permissions updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to updated role:  ${error.data.message}`))
  }
}

export const updateUserPermissionsAsync = (user, permissions) => async (dispatch) => {
  try {
    await updateUserAJAX(user.links.self, user.roles, permissions)
    dispatch(publishNotification('success', 'User permissions updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to updated user:  ${error.data.message}`))
  }
}

export const updateUserRolesAsync = (user, roles) => async (dispatch) => {
  try {
    await updateUserAJAX(user.links.self, roles, user.permissions)
    dispatch(publishNotification('success', 'User roles updated'))
  } catch (error) {
    dispatch(publishNotification('error', `Failed to updated user:  ${error.data.message}`))
  }
}
