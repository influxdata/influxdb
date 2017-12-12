import AJAX from 'src/utils/ajax'

export const getUsers = async url => {
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

export const getOrganizations = async url => {
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
    console.error(error)
    throw error
  }
}

// TODO: change updatedUserWithRolesOnly to a whole user that can have the
// original name, provider, and scheme once the change to allow this is
// implemented server-side
export const updateUser = async updatedUserWithRolesOnly => {
  try {
    return await AJAX({
      method: 'PATCH',
      url: updatedUserWithRolesOnly.links.self,
      data: updatedUserWithRolesOnly,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteUser = async user => {
  try {
    return await AJAX({
      method: 'DELETE',
      url: user.links.self,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const createOrganization = async (url, organization) => {
  try {
    return await AJAX({
      method: 'POST',
      url,
      data: organization,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const updateOrganization = async organization => {
  try {
    return await AJAX({
      method: 'PATCH',
      url: organization.links.self,
      data: organization,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const deleteOrganization = async organization => {
  try {
    return await AJAX({
      method: 'DELETE',
      url: organization.links.self,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}

export const getAuthSettings = async url => {
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

export const updateAuthSettings = async (url, authSettings) => {
  try {
    return await AJAX({
      method: 'PATCH',
      url,
      data: authSettings,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
