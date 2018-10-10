// Libraries
import AJAX from 'src/utils/ajax'

// Utils
import {getDeep} from 'src/utils/wrappers'

// Types
import {View, ViewParams} from 'src/types/v2'

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

export const readViews = async (
  url: string,
  params?: ViewParams
): Promise<View[]> => {
  try {
    const response = await AJAX({
      method: 'GET',
      url,
      params,
    })

    return getDeep<View[]>(response, 'data.views', [])
  } catch (error) {
    console.error(error)
    return []
  }
}
