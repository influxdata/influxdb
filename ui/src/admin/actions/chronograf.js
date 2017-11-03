import {
  getUsers as getUsersAJAX,
  getOrganizations as getOrganizationsAJAX,
  createUser as createUserAJAX,
  updateUser as updateUserAJAX,
  deleteUser as deleteUserAJAX,
  createOrganization as createOrganizationAJAX,
  renameOrganization as renameOrganizationAJAX,
  deleteOrganization as deleteOrganizationAJAX,
} from 'src/admin/apis/chronograf'

import {publishAutoDismissingNotification} from 'shared/dispatchers'
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

export const updateUser = (user, updatedUser) => ({
  type: 'CHRONOGRAF_UPDATE_USER',
  payload: {
    user,
    updatedUser,
  },
})

export const syncUser = (staleUser, syncedUser) => ({
  type: 'CHRONOGRAF_SYNC_USER',
  payload: {
    staleUser,
    syncedUser,
  },
})

export const removeUser = user => ({
  type: 'CHRONOGRAF_REMOVE_USER',
  payload: {
    user,
  },
})

export const addOrganization = organization => ({
  type: 'CHRONOGRAF_ADD_ORGANIZATION',
  payload: {
    organization,
  },
})

export const renameOrganization = (organization, newName) => ({
  type: 'CHRONOGRAF_RENAME_ORGANIZATION',
  payload: {
    organization,
    newName,
  },
})

export const syncOrganization = (staleOrganization, syncedOrganization) => ({
  type: 'CHRONOGRAF_SYNC_ORGANIZATION',
  payload: {
    staleOrganization,
    syncedOrganization,
  },
})

export const removeOrganization = organization => ({
  type: 'CHRONOGRAF_REMOVE_ORGANIZATION',
  payload: {
    organization,
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
    dispatch(removeUser(user))
  }
}

export const updateUserAsync = (user, updatedUser) => async dispatch => {
  dispatch(updateUser(user, updatedUser))
  try {
    // currently the request will be rejected if name, provider, or scheme, or
    // no roles are sent with the request.
    // TODO: remove the null assignments below so that the user request can have
    // the original name, provider, and scheme once the change to allow this is
    // implemented server-side
    const {data} = await updateUserAJAX({
      ...updatedUser,
      name: null,
      provider: null,
      scheme: null,
    })
    // it's not necessary to syncUser again but it's useful for good
    // measure and for the clarity of insight in the redux story
    dispatch(syncUser(user, data))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(syncUser(user, user))
  }
}

export const deleteUserAsync = user => async dispatch => {
  dispatch(removeUser(user))
  try {
    await deleteUserAJAX(user.links.self)
    dispatch(
      publishAutoDismissingNotification(
        'success',
        `User deleted: ${user.scheme}::${user.provider}::${user.name}`
      )
    )
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(addUser(user))
  }
}

export const createOrganizationAsync = (
  url,
  organization
) => async dispatch => {
  dispatch(addOrganization(organization))
  try {
    const {data} = await createOrganizationAJAX(url, organization)
    dispatch(syncOrganization(organization, data))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(removeOrganization(organization))
  }
}

export const renameOrganizationAsync = (
  organization,
  updatedOrganization
) => async dispatch => {
  dispatch(renameOrganization(organization, updatedOrganization.name))
  try {
    const {data} = await renameOrganizationAJAX(updatedOrganization)
    // it's not necessary to syncOrganization again but it's useful for good
    // measure and for the clarity of insight in the redux story
    dispatch(syncOrganization(updatedOrganization, data))
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(syncOrganization(organization, organization)) // restore if fail
  }
}

export const deleteOrganizationAsync = organization => async dispatch => {
  dispatch(removeOrganization(organization))
  try {
    await deleteOrganizationAJAX(organization.links.self)
    dispatch(
      publishAutoDismissingNotification(
        'success',
        `Organization deleted: ${organization.name}`
      )
    )
  } catch (error) {
    dispatch(errorThrown(error))
    dispatch(addOrganization(organization))
  }
}
