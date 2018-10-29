import _ from 'lodash'

import AJAX from 'src/utils/ajax'
import {SchemaFilter} from 'src/types'
import {Source} from 'src/types/v2'

export const measurements = async (
  source: Source,
  db: string
): Promise<any> => {
  const script = `
    from(db:"${db}") 
        |> range(start:-24h) 
        |> group(by:["_measurement"]) 
        |> distinct(column:"_measurement") 
        |> group()
    `

  return metaQuery(source, script)
}

export const tagKeys = async (
  source: Source,
  db: string,
  filter: SchemaFilter[]
): Promise<any> => {
  let tagKeyFilter = ''

  if (filter.length) {
    const predicates = filter.map(({key}) => `r._value != "${key}"`)

    tagKeyFilter = `|> filter(fn: (r) => ${predicates.join(' and ')} )`
  }

  const script = `
    from(db: "${db}")
      |> range(start: -24h)
      ${tagsetFilter(filter)}
     	|> group(none: true)
      |> keys(except:["_time", "_value", "_start", "_stop"])
      |> map(fn: (r) => r._value)
      ${tagKeyFilter}
    `

  return metaQuery(source, script)
}

interface TagValuesParams {
  source: Source
  db: string
  tagKey: string
  limit: number
  filter?: SchemaFilter[]
  searchTerm?: string
  count?: boolean
}

export const tagValues = async ({
  db,
  source,
  tagKey,
  limit,
  filter = [],
  searchTerm = '',
  count = false,
}: TagValuesParams): Promise<any> => {
  let regexFilter = ''

  if (searchTerm) {
    regexFilter = `|> filter(fn: (r) => r.${tagKey} =~ /${searchTerm}/)`
  }

  const limitFunc = count ? '' : `|> limit(n:${limit})`
  const countFunc = count ? '|> count()' : ''

  const script = `
    from(db:"${db}")
      |> range(start:-1h)
      ${regexFilter}
      ${tagsetFilter(filter)}
      |> group(by:["${tagKey}"])
      |> distinct(column:"${tagKey}")
      |> group(by:["_stop","_start"])
      ${limitFunc}
      ${countFunc}
  `

  return metaQuery(source, script)
}

export const tagsFromMeasurement = async (
  source: Source,
  db: string,
  measurement: string
): Promise<any> => {
  const script = `
    from(db:"${db}") 
      |> range(start:-24h) 
      |> filter(fn:(r) => r._measurement == "${measurement}") 
      |> group() 
      |> keys(except:["_time","_value","_start","_stop"])
  `

  return metaQuery(source, script)
}

const tagsetFilter = (filter: SchemaFilter[]): string => {
  if (!filter.length) {
    return ''
  }

  const predicates = filter.map(({key, value}) => `r.${key} == "${value}"`)

  return `|> filter(fn: (r) => ${predicates.join(' and ')} )`
}

const metaQuery = async (source: Source, query: string) => {
  const url = source.links.query
  const type = 'flux'
  const dialect = {
    delimiter: ',',
    header: true,
    annotations: ['group', 'default', 'datatype'],
  }
  try {
    const response = await AJAX({
      method: 'POST',
      url,
      data: {
        type,
        query,
        dialect,
      },
    })

    return response.data
  } catch (error) {
    handleError(error)
  }
}

const handleError = error => {
  console.error('Problem fetching data', error)

  throw _.get(error, 'headers.x-influx-error', false) ||
    _.get(error, 'data.message', 'unknown error 🤷')
}
