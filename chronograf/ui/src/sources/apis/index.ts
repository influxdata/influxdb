import AJAX from 'src/utils/ajax'

export const getSourceHealth = async (url: string) => {
  try {
    await AJAX({url})
  } catch (error) {
    console.error(`Unable to contact source ${url}`, error)
    throw error
  }
}
