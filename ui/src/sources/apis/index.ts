import {Source} from 'src/api'
import {sourcesAPI} from 'src/utils/api'

export const readSources = async (): Promise<Source[]> => {
  const {data} = await sourcesAPI.sourcesGet('')

  return data.sources
}

export const readSource = async (id: string): Promise<Source> => {
  const {data} = await sourcesAPI.sourcesSourceIDGet(id)

  return data
}

export const createSource = async (
  org: string,
  attributes: Partial<Source>
): Promise<Source> => {
  const {data} = await sourcesAPI.sourcesPost(org, attributes)

  return data
}

export const updateSource = async (
  id: string,
  source: Partial<Source>
): Promise<Source> => {
  const {data} = await sourcesAPI.sourcesSourceIDPatch(id, source)

  return data
}

export async function deleteSource(source: Source): Promise<void> {
  await sourcesAPI.sourcesSourceIDDelete(source.id)
}

export const getSourceHealth = async (id: string): Promise<void> => {
  await sourcesAPI.sourcesSourceIDHealthGet(id)
}
