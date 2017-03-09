import AJAX from 'src/utils/ajax'

export const getUsers = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
  }
}

export const getRoles = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
  }
}

export const createUser = async (url, user) => {
  try {
    return await AJAX({
      method: 'POST',
      url,
      data: user,
    })
  } catch (error) {
    throw error
  }
}

export const deleteRole = async (url, addFlashMessage, rolename) => {
  try {
    const response = await AJAX({
      method: 'DELETE',
      url,
    })
    addFlashMessage({
      type: 'success',
      text: `${rolename} successfully deleted.`,
    })
    return response
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
    addFlashMessage({
      type: 'error',
      text: `Error deleting: ${rolename}.`,
    })
  }
}

export const deleteUser = async (url, addFlashMessage, username) => {
  try {
    const response = await AJAX({
      method: 'DELETE',
      url,
    })
    addFlashMessage({
      type: 'success',
      text: `${username} successfully deleted.`,
    })
    return response
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
    addFlashMessage({
      type: 'error',
      text: `Error deleting: ${username}.`,
    })
  }
}

export const addUsersToRole = async (url, users) => {
  try {
    await AJAX({
      method: 'PATCH',
      url,
      data: {
        users,
      },
    })
  } catch (error) {
    console.error(error)
  }
}

export const updateRolePermissions = async (url, permissions) => {
  try {
    await AJAX({
      method: 'PATCH',
      url,
      data: {
        permissions,
      },
    })
  } catch (error) {
    console.error(error)
  }
}
