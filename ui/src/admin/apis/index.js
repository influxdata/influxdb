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

export const deleteRole = async (url) => {
  try {
    return await AJAX({
      method: 'DELETE',
      url,
    })
  } catch (error) {
    console.error(error) // eslint-disable-line no-console
  }
}
