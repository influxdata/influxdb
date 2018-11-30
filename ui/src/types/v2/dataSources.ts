export enum ConfigurationState {
  Unconfigured = 'unconfigured',
  Verifying = 'verifying',
  Configured = 'configured',
  Loading = 'loading',
  Done = 'done',
  Error = 'error',
}

export enum DataLoaderType {
  CSV = 'CSV',
  Streaming = 'Streaming',
  LineProtocol = 'Line Protocol',
  Empty = '',
}

export interface DataSource {
  name: string
  configured: ConfigurationState
  active: boolean
  configs: any
}
