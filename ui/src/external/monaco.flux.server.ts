import Deferred from '../utils/Deferred'
import {
  LSPResponse,
  parseResponse,
  initialize,
  LSPMessage,
  completion,
  didOpen,
  didChange,
  NotificationMessage,
} from 'src/external/monaco.flux.messages'
import {registerCompletion} from './monaco.flux.completions'
import {AppState, LocalStorage} from 'src/types'
import {getVariableAssignments} from '../timeMachine/selectors'
import {buildVarsOption} from '../variables/utils/buildVarsOption'

import {store} from '../index'

import {Store} from 'redux'
import {
  CompletionItem,
  CompletionContext,
  Position,
  Diagnostic,
} from 'monaco-languageclient/lib/services'
import {Server} from '@influxdata/flux-lsp-browser'

type BucketCallback = () => Promise<string[]>

export interface WASMServer extends Server {
  register_buckets_callback: (BucketCallback) => void
}

import {format_from_js_file} from '@influxdata/flux'

export class LSPServer {
  private server: WASMServer
  private messageID: number = 0
  private buckets: string[] = []
  private documentVersions: {[key: string]: number} = {}
  public store: Store<AppState & LocalStorage>

  constructor(server: WASMServer, reduxStore = store) {
    this.server = server
    this.server.register_buckets_callback(this.getBuckets)
    this.store = reduxStore
  }

  getBuckets = () => {
    return Promise.resolve(this.buckets)
  }

  updateBuckets(buckets: string[]) {
    this.buckets = buckets
  }

  initialize() {
    return this.send(initialize(this.currentMessageID))
  }

  async completionItems(
    uri: string,
    position: Position,
    context: CompletionContext
  ): Promise<CompletionItem[]> {
    await this.sendPrelude(uri)

    try {
      const response = (await this.send(
        completion(
          this.currentMessageID,
          uri,
          {
            ...position,
            line: position.line,
          },
          context
        )
      )) as {result?: {items?: []}}

      return (response.result || {}).items || []
    } catch (e) {
      return []
    }
  }

  async didOpen(uri: string, script: string) {
    await this.sendPrelude(uri)
    const response = await this.send(
      didOpen(this.currentMessageID, uri, script, 1)
    )
    this.documentVersions[uri] = 1

    return this.parseDiagnostics(response as NotificationMessage)
  }

  async didChange(uri: string, script: string) {
    await this.sendPrelude(uri)
    const version = this.documentVersions[uri] || 1
    const response = await this.send(
      didChange(this.currentMessageID, uri, script, version + 1)
    )
    this.documentVersions[uri] = version + 1

    return this.parseDiagnostics(response as NotificationMessage)
  }

  private parseDiagnostics(response: NotificationMessage): Diagnostic[] {
    if (
      response.method === 'textDocument/publishDiagnostics' &&
      response.params
    ) {
      const {diagnostics} = response.params as {diagnostics: Diagnostic[]}

      return diagnostics || []
    }

    return []
  }

  private get currentMessageID(): number {
    const result = this.messageID
    this.messageID++
    return result
  }

  private async send(message: LSPMessage): Promise<LSPResponse> {
    const body = JSON.stringify(message)
    const fullMessage = `Content-Length: ${body.length}\r\n\r\n${body}`
    const response = await this.server.process(fullMessage)

    return parseResponse(response)
  }

  private async sendPrelude(uri: string): Promise<void> {
    const state = this.store.getState()
    const variableAssignments = getVariableAssignments(state)
    const file = buildVarsOption(variableAssignments)

    const parts = uri.split('/')
    parts.pop()
    const dir = parts.join('/')
    const path = `${dir}/prelude.flux`

    const prelude = format_from_js_file(file)
    await this.send(didOpen(this.currentMessageID, path, prelude, 0))
  }
}

class LSPLoader {
  private server: LSPServer
  private queue: Deferred<LSPServer>[] = []
  private loading: boolean = false

  async load() {
    if (this.server) {
      return this.server
    }

    const deferred = new Deferred<LSPServer>()

    if (this.loading) {
      this.queue.push(deferred)
      return await deferred.promise
    }

    this.loading = true

    const {Server} = await import('@influxdata/flux-lsp-browser')
    this.server = new LSPServer(new Server(false, true) as WASMServer)
    registerCompletion(window.monaco, this.server)

    await this.server.initialize()

    this.queue.forEach(d => d.resolve(this.server))

    return this.server
  }
}

const serverLoader = new LSPLoader()

export default async function loader(): Promise<LSPServer> {
  return await serverLoader.load()
}
