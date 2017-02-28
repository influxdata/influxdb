import {getUsers} from 'src/users/apis'

export const loadUsers = (users) => ({
  type: 'LOAD_USERS',
  payload: {
    users,
  },
})

export const loadUsersAsync = (url) => async (dispatch) => {
  const users = await getUsers(url)
  dispatch(loadUsers(users))
}
