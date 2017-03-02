import {getUsers} from 'src/admin/apis'

export const loadUsers = ({users}) => ({
  type: 'LOAD_USERS',
  payload: {
    users,
  },
})

export const loadUsersAsync = (url) => async (dispatch) => {
  const {data} = await getUsers(url)
  dispatch(loadUsers(data))
}

export const loadRoles = ({roles}) => ({
  type: 'LOAD_ROLES',
  payload: {
    roles,
  },
})

export const loadRolesAsync = (url) => async (dispatch) => {
  const {data} = await getUsers(url)
  dispatch(loadRoles(data))
}
