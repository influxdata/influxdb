// Types
import {Variable, RemoteDataState} from 'src/types'

export const createVariable = (
  name: string,
  query: string,
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'query',
    values: {
      query,
      language: 'flux',
    },
  },
  status: RemoteDataState.Done,
})

export const createMapVariable = (
  name: string,
  map: {[key: string]: string} = {},
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'map',
    values: {...map},
  },
  status: RemoteDataState.Done,
})
