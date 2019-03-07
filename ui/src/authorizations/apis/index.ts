import AJAX from 'src/utils/ajax'

export const createAuthorization = async authorization => {
  try {
    return await AJAX({
      method: 'POST',
      url: '/api/v2/authorizations',
      data: authorization,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
