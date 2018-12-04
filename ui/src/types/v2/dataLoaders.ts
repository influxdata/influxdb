// Types
import {TelegrafRequestPlugins} from 'src/api'

export enum ConfigurationState {
  Unconfigured = 'unconfigured',
  Configured = 'configured',
}

export enum DataLoaderType {
  CSV = 'CSV',
  Streaming = 'Streaming',
  LineProtocol = 'Line Protocol',
  Empty = '',
}

export interface TelegrafPlugin extends TelegrafRequestPlugins {
  configured: ConfigurationState
  active: boolean
}
