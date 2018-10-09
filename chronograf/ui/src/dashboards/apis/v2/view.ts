// Libraries
import AJAX from 'src/utils/ajax'

// Types
import {View} from 'src/types/v2'

export const readView = async (url: string): Promise<View> => {
  const {data} = await AJAX({url})

  return data
}

export const createView = async (
  url: string,
  view: Partial<View>
): Promise<View> => {
  const {data} = await AJAX({
    url,
    method: 'POST',
    data: view,
  })

  return data
}

export const updateView = async (
  url: string,
  view: Partial<View>
): Promise<View> => {
  const {data} = await AJAX({
    url,
    method: 'PATCH',
    data: view,
  })

  return data
}
