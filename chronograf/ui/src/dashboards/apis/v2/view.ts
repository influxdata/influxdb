// Libraries
import AJAX from 'src/utils/ajax'

// Types
import {View} from 'src/types/v2'

export const getView = async (url: string): Promise<View> => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data
  } catch (error) {
    throw error
  }
}
