import AJAX from 'src/utils/ajax'
import {Source} from 'src/types/v2'

export const getSources = async (): Promise<Source[]> => {
  try {
    const {data} = await AJAX({
      url: '/api/v2/sources',
    })

    return data.sources
  } catch (error) {
    throw error
  }
}

export const getSource = async (url: string): Promise<Source> => {
  try {
    const {data: source} = await AJAX({
      url,
    })

    return source
  } catch (error) {
    throw error
  }
}

export const createSource = async (
  url: string,
  attributes: Partial<Source>
): Promise<Source> => {
  try {
    const {data: source} = await AJAX({
      url,
      method: 'POST',
      data: attributes,
    })

    return source
  } catch (error) {
    throw error
  }
}

export const updateSource = async (
  newSource: Partial<Source>
): Promise<Source> => {
  try {
    const {data: source} = await AJAX({
      url: newSource.links.self,
      method: 'PATCH',
      data: newSource,
    })

    return source
  } catch (error) {
    throw error
  }
}

export function deleteSource(source) {
  return AJAX({
    url: source.links.self,
    method: 'DELETE',
  })
}

export const getSourceHealth = async (url: string): Promise<void> => {
  try {
    await AJAX({url})
  } catch (error) {
    console.error(`Unable to contact source ${url}`, error)
    throw error
  }
}
