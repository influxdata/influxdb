// Libraries
import Client from '@influxdata/influx'
import {get} from 'lodash'

const basePath = '/api/v2'

export const getErrorMessage = (e: any) => {
  let message = get(e, 'response.data.error.message', '')

  if (message === '') {
    message = get(e, 'response.data.error', '')
  }

  if (message === '') {
    message = get(e, 'response.headers.x-influx-error', '')
  }

  if (message === '') {
    message = get(e, 'response.data.message', '')
  }

  if (message === '') {
    message = 'unknown error'
  }

  return message
}

export const client = new Client(basePath)
