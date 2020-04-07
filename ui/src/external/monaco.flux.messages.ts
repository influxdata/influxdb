import {ServerResponse} from 'src/types'
import {
  CompletionContext,
  ReferenceContext,
  Position,
  SignatureHelpContext,
} from 'monaco-languageclient/lib/services'

interface Message {
  jsonrpc: string
}

export interface ResponseMessage extends Message {
  id: number | string | null
  result?: string | number | boolean | object | null
}

export interface NotificationMessage extends Message {
  method: string
  params?: object[] | object
}

export type LSPResponse = NotificationMessage | ResponseMessage

export type LSPMessage =
  | ReturnType<typeof initialize>
  | ReturnType<typeof didOpen>
  | ReturnType<typeof didChange>
  | ReturnType<typeof completion>

const JSONRPC = '2.0',
  FLUXLANGID = 'flux'

const createRequest = (id: number, method: string, params: object = {}) => {
  return {
    jsonrpc: JSONRPC,
    id,
    method,
    params,
  }
}

export const initialize = (id: number) => {
  return createRequest(id, 'initialize')
}

export const didOpen = (
  id: number,
  uri: string,
  text: string,
  version: number
) => {
  return createRequest(id, 'textDocument/didOpen', {
    textDocument: {
      uri,
      languageId: FLUXLANGID,
      version,
      text,
    },
  })
}

export const didChange = (
  id: number,
  uri: string,
  newText: string,
  version: number
) => {
  return createRequest(id, 'textDocument/didChange', {
    textDocument: {
      uri: uri,
      version: version,
    },
    contentChanges: [
      {
        text: newText,
      },
    ],
  })
}

export const rename = (
  id: number,
  uri: string,
  position: Position,
  newName: string
) => {
  return createRequest(id, 'textDocument/rename', {
    textDocument: {uri},
    position,
    newName,
  })
}

export const references = (
  id: number,
  uri: string,
  position: Position,
  context: ReferenceContext
) => {
  return createRequest(id, 'textDocument/references', {
    textDocument: {uri},
    position,
    context,
  })
}

export const definition = (id: number, uri: string, position: Position) => {
  return createRequest(id, 'textDocument/definition', {
    textDocument: {uri},
    position,
  })
}

export const symbols = (id: number, uri: string) => {
  return createRequest(id, 'textDocument/documentSymbol', {
    textDocument: {uri},
  })
}

export const completion = (
  id: number,
  uri: string,
  position: Position,
  context: CompletionContext
) => {
  return createRequest(id, 'textDocument/completion', {
    textDocument: {uri},
    position,
    context,
  })
}

export const foldingRange = (id, uri) => {
  return createRequest(id, 'textDocument/foldingRange', {
    textDocument: {uri},
  })
}

export const signatureHelp = (
  id: number,
  uri: string,
  position: Position,
  context: SignatureHelpContext
) => {
  return createRequest(id, 'textDocument/signatureHelp', {
    textDocument: {uri},
    position,
    context: {
      isRetrigger: false,
      ...context,
    },
  })
}

export const parseResponse = (response: ServerResponse): LSPResponse => {
  const message = response.get_message()
  const error = response.get_error()

  if (error) {
    throw new Error(error)
  }

  const split = (message || '').split('\r\n')

  try {
    return JSON.parse(split.slice(2).join('\n'))
  } catch (e) {
    throw new Error('failed to parse LSP response')
  }
}

export async function sendMessage(message: LSPMessage, server) {
  const stringifiedMessage = JSON.stringify(message),
    size = stringifiedMessage.length

  const fullMessage = `Content-Length: ${size}\r\n\r\n${stringifiedMessage}`
  const response = await server.process(fullMessage)

  return parseResponse(response)
}
