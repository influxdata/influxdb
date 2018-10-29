import _ from 'lodash'

import AJAX from 'src/utils/ajax'
import {FluxTable} from 'src/types'
import {getDeep} from 'src/utils/wrappers'
import {
  parseResponse,
  parseResponseError,
} from 'src/shared/parsing/flux/response'
import {MAX_RESPONSE_BYTES} from 'src/flux/constants'

export const getSuggestions = async (url: string) => {
  try {
    const {data} = await AJAX({
      url,
    })

    return data.funcs
  } catch (error) {
    console.error('Could not get suggestions', error)
    throw error
  }
}

interface ASTRequest {
  url: string
  query: string
}

export const getAST = async (request: ASTRequest) => {
  const {url, query} = request
  try {
    const {data} = await AJAX({
      method: 'POST',
      url,
      data: {query},
    })

    return getDeep<ASTRequest>(data, 'ast', {url: '', query: ''})
  } catch (error) {
    console.error('Could not parse query', error)
    throw error
  }
}

export interface GetTimeSeriesResult {
  didTruncate: boolean
  tables: FluxTable[]
}

export const getTimeSeries = async (
  url: string, // query URI
  query: string
): Promise<GetTimeSeriesResult> => {
  let responseBody: string
  let responseByteLength: number
  const type = 'flux'
  const dialect = {
    delimiter: ',',
    header: true,
    annotations: ['datatype', 'group', 'default'],
  }

  try {
    // We are using the `fetch` API here since the `AJAX` utility lacks support
    // for limiting response size. The `AJAX` utility depends on
    // `axios.request` which _does_ have a `maxContentLength` option, though it
    // seems to be broken at the moment. We might use this option instead of
    // the `fetch` API in the future, if it is ever fixed.  See
    // https://github.com/axios/axios/issues/1491.
    const resp = await fetch(url, {
      method: 'POST',
      body: JSON.stringify({query, type, dialect}),
    })

    const {body, byteLength} = await decodeFluxRespWithLimit(resp)

    responseBody = body
    responseByteLength = byteLength
  } catch (error) {
    console.error('Problem fetching data', error)

    throw _.get(error, 'headers.x-influx-error', false) ||
      _.get(error, 'data.message', 'unknown error 🤷')
  }

  try {
    return {
      tables: parseResponse(responseBody),
      didTruncate: responseByteLength >= MAX_RESPONSE_BYTES,
    }
  } catch (error) {
    console.error('Could not parse response body', error)

    return {
      tables: parseResponseError(responseBody),
      didTruncate: false,
    }
  }
}

interface DecodeFluxRespWithLimitResult {
  body: string
  byteLength: number
}

const decodeFluxRespWithLimit = async (
  resp: Response
): Promise<DecodeFluxRespWithLimitResult> => {
  const reader = resp.body.getReader()
  const decoder = new TextDecoder()

  let bytesRead = 0
  let body = ''
  let currentRead = await reader.read()

  while (!currentRead.done) {
    const currentText = decoder.decode(currentRead.value)

    bytesRead += currentRead.value.byteLength

    if (bytesRead >= MAX_RESPONSE_BYTES) {
      // Discard last line since it may be partially read
      const lines = currentText.split('\n')
      body += lines.slice(0, lines.length - 1).join('\n')

      reader.cancel()

      return {body, byteLength: bytesRead}
    } else {
      body += currentText
    }

    currentRead = await reader.read()
  }

  reader.cancel()

  return {body, byteLength: bytesRead}
}
