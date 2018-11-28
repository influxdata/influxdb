// Utils
import {viewsAPI} from 'src/utils/api'

// Types
import {View} from 'src/api'
import {NewView} from 'src/types/v2/dashboards'

export const readView = async (id: string): Promise<View> => {
  const {data} = await viewsAPI.viewsViewIDGet(id)

  return data
}

export const createView = async (
  view: NewView,
  org: string = ''
): Promise<View> => {
  const {data} = await viewsAPI.viewsPost(org, view)

  return data
}

export const updateView = async (
  id: string,
  view: Partial<View>
): Promise<View> => {
  const {data} = await viewsAPI.viewsViewIDPatch(id, view)

  return data
}

export const readViews = async (org: string = ''): Promise<View[]> => {
  const {data} = await viewsAPI.viewsGet(org)
  return data.views
}
