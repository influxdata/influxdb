export enum ConfigurationState {
  Unconfigured = 'unconfigured',
  Verifying = 'verifying',
  Configured = 'configured',
  Loading = 'loading',
  Done = 'done',
  Error = 'error',
}

export interface DataSource {
  name: string
  configured: ConfigurationState
  active: boolean
  configs: any
}
