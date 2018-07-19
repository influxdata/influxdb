import AJAX from 'src/utils/ajax'

export function getMe() {
  return AJAX({
    resource: 'me',
    method: 'GET',
  })
}

export const updateMe = async (url, updatedMe) => {
  try {
    return await AJAX({
      method: 'PUT',
      url,
      data: updatedMe,
    })
  } catch (error) {
    console.error(error)
    throw error
  }
}
