import {ServerResponse} from 'src/types'

type LSPMessage =
  | typeof initialize
  | ReturnType<typeof didOpen>
  | ReturnType<typeof didChange>
  | ReturnType<typeof completion>

const JSONRPC = '2.0',
  FLUXLANGID = 'flux'

export const initialize = {
  jsonrpc: JSONRPC,
  id: 1,
  method: 'initialize',
  params: {},
} as const

export const didOpen = (uri, text) => ({
  jsonrpc: JSONRPC,
  id: 2,
  method: 'textDocument/didOpen' as const,
  params: {
    textDocument: {
      uri: uri,
      languageId: FLUXLANGID,
      version: 1 as const,
      text,
    },
  },
})

export const didChange = (uri, newText, version, messageID) => ({
  jsonrpc: JSONRPC,
  id: messageID,
  method: 'textDocument/didChange' as const,
  params: {
    textDocument: {
      uri: uri,
      version: version,
    },
    contentChanges: [
      {
        text: newText,
      },
    ],
  },
})

export const completion = (uri, position, context) => ({
  jsonrpc: JSONRPC,
  id: 100,
  method: 'textDocument/completion' as const,
  params: {
    textDocument: {uri: uri},
    position,
    context,
  },
})

export const parseResponse = (response: ServerResponse): object => {
  const message = response.get_message(),
    error = response.get_error()
  let split

  if (message) {
    split = message.split('\n')

    if (split.length >= 2) {
      return JSON.parse(split[2])
    }

    return {items: []}
  }

  split = error.split('\n')

  if (split.length >= 2) {
    return JSON.parse(split[2])
  }

  return {}
}

export function sendMessage(message: LSPMessage, server) {
  const stringifiedMessage = JSON.stringify(message),
    size = stringifiedMessage.length

  return server
    .process(`Content-Length: ${size}\r\n\r\n` + stringifiedMessage)
    .then(resp => {
      return parseResponse(resp)
    })
}
