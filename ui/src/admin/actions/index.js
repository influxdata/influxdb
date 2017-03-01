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
