import {get} from 'lodash'

// Constants
import {FLUXLANGID} from 'src/external/constants'

// Types
import {ServerResponse} from 'src/types'

type LSPMessage =
  | typeof initialize
  | ReturnType<typeof didOpen>
  | ReturnType<typeof didChange>
  | ReturnType<typeof completion>

const URI = 'monacoeditor' as const
const JSONRPC = '2.0' as const

export const initialize = {
  jsonrpc: JSONRPC,
  id: 1,
  method: 'initialize',
  params: {},
} as const

export const didOpen = (text: string) => ({
  jsonrpc: JSONRPC,
  id: 2,
  method: 'textDocument/didOpen' as const,
  params: {
    textDocument: {
      uri: URI,
      languageId: FLUXLANGID,
      version: 1 as const,
      text,
    },
  },
})

export const didChange = (
  newText: string,
  version: number,
  messageID: number
) => ({
  jsonrpc: JSONRPC,
  id: messageID,
  method: 'textDocument/didChange' as const,
  params: {
    textDocument: {
      uri: URI,
      version: version,
    },
    contentChanges: [
      {
        text: newText,
      },
    ],
  },
})

export const completion = (position, context) => ({
  jsonrpc: JSONRPC,
  id: 100,
  method: 'textDocument/completion' as const,
  params: {
    textDocument: {uri: URI},
    position,
    context,
  },
})

export const parseResponse = (response: ServerResponse): null | object => {
  const message = response.get_message()
  if (message) {
    const split = message.split('\n')
    const parsed_msg = get(split, '2', null)
    return JSON.parse(parsed_msg)
  } else {
    const error = response.get_error()
    const split = error.split('\n')
    const parsed_err = get(split, '2', null)
    return JSON.parse(parsed_err)
  }
}

export const sendMessage = (message: LSPMessage, server) => {
  const stringifiedMessage = JSON.stringify(message)
  const size = stringifiedMessage.length

  const resp = server.process(
    `Content-Length: ${size}\r\n\r\n` + stringifiedMessage
  )

  return parseResponse(resp)
}
