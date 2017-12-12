import AJAX from 'src/utils/ajax'

export const getAuthConfig = async url => {
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

export const updateAuthConfig = async (url, authConfig) => {
  try {
    return await AJAX({
      method: 'PATCH',
      url,
      data: authConfig,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
