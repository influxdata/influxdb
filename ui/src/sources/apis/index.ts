import AJAX from 'src/utils/ajax'
import {Source} from 'src/types/v2'

export const readSources = async (url): Promise<Source[]> => {
  const {data} = await AJAX({url})

  return data.sources
}

export const readSource = async (url: string): Promise<Source> => {
  const {data: source} = await AJAX({
    url,
  })

  return source
}

export const createSource = async (
  url: string,
  attributes: Partial<Source>
): Promise<Source> => {
  const {data: source} = await AJAX({
    url,
    method: 'POST',
    data: attributes,
  })

  return source
}

export const updateSource = async (
  newSource: Partial<Source>
): Promise<Source> => {
  const {data: source} = await AJAX({
    url: newSource.links.self,
    method: 'PATCH',
    data: newSource,
  })

  return source
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
