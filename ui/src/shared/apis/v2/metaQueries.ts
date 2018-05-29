import _ from 'lodash'

import AJAX from 'src/utils/ajax'
import {Service, SchemaFilter} from 'src/types'

export const measurements = async (
  service: Service,
  db: string
): Promise<any> => {
  const script = `
    from(db:"${db}") 
        |> range(start:-24h) 
        |> group(by:["_measurement"]) 
        |> distinct(column:"_measurement") 
        |> group()
    `

  return proxy(service, script)
}

export const tagKeys = async (
  service: Service,
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

  return proxy(service, script)
}

export const tagValues = async (
  service: Service,
  db: string,
  filter: SchemaFilter[],
  tagKey: string,
  searchTerm: string = ''
): Promise<any> => {
  let regexFilter = ''

  if (searchTerm) {
    regexFilter = `|> filter(fn: (r) => r.${tagKey} =~ /${searchTerm}/)`
  }

  const script = `
    from(db:"${db}")
      |> range(start:-1h)
      ${regexFilter}
      ${tagsetFilter(filter)}
      |> group(by:["${tagKey}"])
      |> distinct(column:"${tagKey}")
      |> group(none: true)
      |> limit(n:100)
  `

  return proxy(service, script)
}

export const tagsFromMeasurement = async (
  service: Service,
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

  return proxy(service, script)
}

const tagsetFilter = (filter: SchemaFilter[]): string => {
  if (!filter.length) {
    return ''
  }

  const predicates = filter.map(({key, value}) => `r.${key} == "${value}"`)

  return `|> filter(fn: (r) => ${predicates.join(' and ')} )`
}

const proxy = async (service: Service, script: string) => {
  const and = encodeURIComponent('&')
  const mark = encodeURIComponent('?')
  const garbage = script.replace(/\s/g, '') // server cannot handle whitespace

  try {
    const response = await AJAX({
      method: 'POST',
      url: `${
        service.links.proxy
      }?path=/v1/query${mark}orgName=defaulorgname${and}q=${garbage}`,
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
