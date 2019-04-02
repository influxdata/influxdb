// Types
import {Variable} from '@influxdata/influx'

export const createVariable = (
  name: string,
  query: string,
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  arguments: {
    type: 'query',
    values: {
      query,
      language: 'flux',
    },
  },
})
