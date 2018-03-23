import AJAX from 'src/utils/ajax'

export const getSuggestions = async (url: string) => {
  try {
    return await AJAX({
      url,
    })
  } catch (error) {
    console.error('Could not get suggestions', error)
    throw error
  }
}
