import {
  getUsers as getUsersAJAX,
  getOrganizations as getOrganizationsAJAX,
  createUser as createUserAJAX,
} from 'src/admin/apis/chronograf'

import {errorThrown} from 'shared/actions/errors'

// action creators

// response contains `users` and `links`
export const loadUsers = ({users}) => ({
  type: 'CHRONOGRAF_LOAD_USERS',
  payload: {
    users,
  },
})

export const loadOrganizations = ({organizations}) => ({
  type: 'CHRONOGRAF_LOAD_ORGANIZATIONS',
  payload: {
    organizations,
  },
})

export const addUser = user => ({
  type: 'CHRONOGRAF_ADD_USER',
  payload: {
    user,
  },
})

export const syncUser = (staleUser, syncedUser) => ({
  type: 'CHRONOGRAF_SYNC_USER',
  payload: {
    staleUser,
    syncedUser,
  },
})

export const deleteUser = user => ({
  type: 'CHRONOGRAF_DELETE_USER',
  payload: {
    user,
  },
})

// async actions (thunks)
export const loadUsersAsync = url => async dispatch => {
  try {
    const {data} = await getUsersAJAX(url)
    dispatch(loadUsers(data))
  } catch (error) {
    dispatch(errorThrown(error))
  }
}

export const loadOrganizationsAsync = url => async dispatch => {
  try {
    const {data} = await getOrganizationsAJAX(url)
    dispatch(loadOrganizations(data))
  } catch (error) {
    dispatch(errorThrown(error))
  }
}

export const createUserAsync = (url, user) => async dispatch => {
  dispatch(addUser(user))
  try {
    const {data} = await createUserAJAX(url, user)
    dispatch(syncUser(user, data))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(deleteUser(user))
  }
}
