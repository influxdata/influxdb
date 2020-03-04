import Deferred from '../utils/Deferred'
import {
  LSPResponse,
  parseResponse,
  initialize,
  LSPMessage,
  completion,
  didOpen,
  didChange,
} from 'src/external/monaco.flux.messages'
import {registerCompletion} from './monaco.flux.completions'

import {
  CompletionItem,
  CompletionContext,
  Position,
} from 'monaco-languageclient/lib/services'
import {Server} from '@influxdata/flux-lsp-browser'

type BucketCallback = () => Promise<string[]>

export interface WASMServer extends Server {
  register_buckets_callback: (BucketCallback) => void
}

const queue = []
let server = null,
  loading = false

export class LSPServer {
  private server: WASMServer
  private messageID: number = 0
  private buckets: string[] = []
  private documentVersions: {[key: string]: number} = {}

  constructor(server: WASMServer) {
    this.server = server
    this.server.register_buckets_callback(this.getBuckets)
  }

  getBuckets = () => {
    return Promise.resolve(this.buckets)
  }

  updateBuckets(buckets: string[]) {
    this.buckets = buckets
  }

  async send(message: LSPMessage): Promise<LSPResponse> {
    const body = JSON.stringify(message)
    const fullMessage = `Content-Length: ${body.length}\r\n\r\n${body}`
    const response = await this.server.process(fullMessage)

    return parseResponse(response)
  }

  initialize() {
    return this.send(initialize(this.currentMessageID))
  }

  async completionItems(
    uri: string,
    position: Position,
    context: CompletionContext
  ): Promise<CompletionItem[]> {
    try {
      const response = (await this.send(
        completion(this.currentMessageID, uri, position, context)
      )) as {result?: {items?: []}}

      return (response.result || {}).items || []
    } catch (e) {
      return []
    }
  }

  async didOpen(uri: string, script: string) {
    await this.send(didOpen(this.currentMessageID, uri, script, 1))
    this.documentVersions[uri] = 1
  }

  async didChange(uri: string, script: string) {
    const version = this.documentVersions[uri] || 1
    await this.send(didChange(this.currentMessageID, uri, script, version + 1))
    this.documentVersions[uri] = version + 1
  }

  private get currentMessageID(): number {
    const result = this.messageID
    this.messageID++
    return result
  }
}

export default async function loader(): Promise<LSPServer> {
  if (server) {
    return server
  }

  const deferred = new Deferred<LSPServer>()

  if (loading) {
    queue.push(deferred)
    return await deferred.promise
  }

  loading = true

  const {Server} = await import('@influxdata/flux-lsp-browser')
  server = new LSPServer(new Server(false) as WASMServer)
  registerCompletion(window.monaco, server)

  await server.initialize()

  queue.forEach(d => d.resolve(server))

  return server
}
