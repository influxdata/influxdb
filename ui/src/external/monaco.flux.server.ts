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
  signatureHelp,
  foldingRange,
  rename,
  references,
  definition,
  symbols,
  formatting,
} from 'src/external/monaco.flux.messages'
import {registerCompletion} from 'src/external/monaco.flux.lsp'
import {AppState, LocalStorage} from 'src/types'
import {getAllVariables, asAssignment} from 'src/variables/selectors'
import {buildVarsOption} from 'src/variables/utils/buildVarsOption'
import {runQuery} from 'src/shared/apis/query'
import {parseResponse as parse} from 'src/shared/parsing/flux/response'

import {store} from 'src/index'

import {Store} from 'redux'
import {
  CompletionItem,
  CompletionContext,
  SignatureHelp,
  Position,
  Diagnostic,
  FoldingRange,
  WorkspaceEdit,
  Location,
  SymbolInformation,
  TextEdit,
} from 'monaco-languageclient/lib/services'
import {Server} from '@influxdata/flux-lsp-browser'

type BucketCallback = () => Promise<string[]>
type MeasurementsCallback = (bucket: string) => Promise<string[]>
type TagKeysCallback = (bucket: string) => Promise<string[]>

export interface WASMServer extends Server {
  register_buckets_callback: (BucketCallback) => void
  register_measurements_callback: (MeasurementsCallback) => void
  register_tag_keys_callback: (TagKeysCallback) => void
}

import {format_from_js_file} from '@influxdata/flux'

// NOTE: parses table then select measurements from the _value column
const parseQueryResponse = response => {
  const data = (parse(response.csv) || [{data: [{}]}])[0].data
  return data.slice(1).map(r => r[3])
}

const queryMeasurements = async (orgID, bucket) => {
  if (!orgID || orgID === '') {
    throw new Error('no org is provided')
  }

  const query = `import "influxdata/influxdb/v1"
      v1.measurements(bucket:"${bucket}")`

  const raw = await runQuery(orgID, query).promise
  if (raw.type !== 'SUCCESS') {
    throw new Error('failed to get measurements')
  }

  return raw
}

const queryTagKeys = async (orgID, bucket) => {
  if (!orgID || orgID === '') {
    throw new Error('no org is provided')
  }

  const query = `import "influxdata/influxdb/v1"
      v1.tagKeys(bucket:"${bucket}")`

  const raw = await runQuery(orgID, query).promise
  if (raw.type !== 'SUCCESS') {
    throw new Error('failed to get tagKeys')
  }

  return raw
}

export class LSPServer {
  private server: WASMServer
  private messageID: number = 0
  private buckets: string[] = []
  private orgID: string = ''
  private documentVersions: {[key: string]: number} = {}
  public store: Store<AppState & LocalStorage>

  constructor(server: WASMServer, reduxStore = store) {
    this.server = server
    this.server.register_buckets_callback(this.getBuckets)
    this.server.register_measurements_callback(this.getMeasurements)
    this.server.register_tag_keys_callback(this.getTagKeys)
    this.store = reduxStore
  }

  getTagKeys = async bucket => {
    try {
      const response = await queryTagKeys(this.orgID, bucket)
      return parseQueryResponse(response)
    } catch (e) {
      console.error(e)
      return []
    }
  }

  getBuckets = () => {
    return Promise.resolve(this.buckets)
  }

  getMeasurements = async (bucket: string) => {
    try {
      const response = await queryMeasurements(this.orgID, bucket)
      return parseQueryResponse(response)
    } catch (e) {
      console.error(e)
      return []
    }
  }

  updateBuckets(buckets: string[]) {
    this.buckets = buckets
  }

  setOrg(orgID: string) {
    this.orgID = orgID
  }

  initialize() {
    return this.send(initialize(this.currentMessageID))
  }

  async rename(uri, position, newName): Promise<WorkspaceEdit> {
    const response = (await this.send(
      rename(this.currentMessageID, uri, position, newName)
    )) as {result: WorkspaceEdit}

    return response.result
  }

  async definition(uri, position): Promise<Location> {
    const response = (await this.send(
      definition(this.currentMessageID, uri, position)
    )) as {result: Location}

    return response.result
  }

  async symbols(uri): Promise<SymbolInformation[]> {
    const response = (await this.send(symbols(this.currentMessageID, uri))) as {
      result: SymbolInformation[]
    }

    return response.result
  }

  async references(uri, position, context): Promise<Location[]> {
    const response = (await this.send(
      references(this.currentMessageID, uri, position, context)
    )) as {result: Location[]}

    return response.result
  }

  async foldingRanges(uri): Promise<FoldingRange[]> {
    const response = (await this.send(
      foldingRange(this.currentMessageID, uri)
    )) as {result: FoldingRange[]}

    return response.result
  }

  async signatureHelp(uri, position, context): Promise<SignatureHelp> {
    await this.sendPrelude(uri)

    const response = (await this.send(
      signatureHelp(this.currentMessageID, uri, position, context)
    )) as {result: SignatureHelp}

    return response.result
  }

  async formatting(uri): Promise<TextEdit[]> {
    await this.sendPrelude(uri)

    const response = (await this.send(
      formatting(this.currentMessageID, uri)
    )) as {result: TextEdit[]}

    return response.result
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
          {...position, line: position.line},
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

    // NOTE: we use the AST intermediate format as a means of reducing
    // drift between the parser and the internal representation
    const variables = getAllVariables(state)
      .map(v => asAssignment(v))
      .filter(v => !!v)

    const file = buildVarsOption(variables)

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
