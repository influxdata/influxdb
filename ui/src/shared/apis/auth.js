import AJAX from 'src/utils/ajax'

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
