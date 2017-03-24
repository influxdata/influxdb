import AJAX from 'src/utils/ajax'

export const getUsers = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getRoles = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getPermissions = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getDbsAndRps = async (url) => {
  try {
    return await AJAX({
      method: 'GET',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
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

export const createRole = async (url, role) => {
  try {
    return await AJAX({
      method: 'POST',
      url,
      data: role,
    })
  } catch (error) {
    throw error
  }
}

export const createDatabase = async (url, database) => {
  try {
    return await AJAX({
      method: 'POST',
      url,
      data: database,
    })
  } catch (error) {
    throw error
  }
}

export const createRetentionPolicy = async (url, retentionPolicy) => {
  try {
    return await AJAX({
      method: 'POST',
      url,
      data: retentionPolicy,
    })
  } catch (error) {
    throw error
  }
}

export const deleteRetentionPolicy = async (url) => {
  try {
    return await AJAX({
      method: 'DELETE',
      url,
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
    console.error(error)
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
    console.error(error)
    addFlashMessage({
      type: 'error',
      text: `Error deleting: ${username}.`,
    })
  }
}

export const deleteDatabase = async (url) => {
  try {
    return await AJAX({
      method: 'DELETE',
      url,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateRole = async (url, users, permissions) => {
  try {
    return await AJAX({
      method: 'PATCH',
      url,
      data: {
        users,
        permissions,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateUser = async (url, roles, permissions) => {
  try {
    return await AJAX({
      method: 'PATCH',
      url,
      data: {
        roles,
        permissions,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateRetentionPolicy = async (url, retentionPolicy) => {
  try {
    return await AJAX({
      method: 'PUT',
      url,
      data: {
        ...retentionPolicy,
      },
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
