import _ from 'lodash'

import AJAX from 'src/utils/ajax'
import {Service} from 'src/types'

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

export const tags = async (service: Service, db: string): Promise<any> => {
  const script = `
    from(db: "${db}")
	    |> range(start: -24h)
     	|> group(none: true)
      |> keys(except:["_time","_value","_start","_stop"])
      |> map(fn: (r) => r._value)
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
