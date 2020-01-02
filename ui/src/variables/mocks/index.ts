// Types
import {Variable} from 'src/types'

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
})
