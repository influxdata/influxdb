import _ from 'lodash'

import AJAX from 'src/utils/ajax'
import {Service} from 'src/types'

const sendIFQLRequest = (service: Service, script: string) => {
  const and = encodeURIComponent('&')
  const mark = encodeURIComponent('?')
  const garbage = script.replace(/\s/g, '') // server cannot handle whitespace

  return AJAX({
    method: 'POST',
    url: `${
      service.links.proxy
    }?path=/v1/query${mark}orgName=defaulorgname${and}q=${garbage}`,
  })
}

export const measurements = async (
  service: Service,
  db: string
): Promise<any> => {
  const script = `
    from(db:"${db}") 
        |> range(start:-1h) 
        |> group(by:["_measurement"]) 
        |> distinct(column:"_measurement") 
        |> group()
    `

  try {
    const response = await sendIFQLRequest(service, script)

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
