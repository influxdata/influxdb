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

export enum LineProtocolTab {
  UploadFile = 'uploadFile',
  EnterManually = 'enterManually',
  EnterURL = 'enterURL',
}

export enum LineProtocolStatus {
  ImportData = 'importData',
  Loading = 'enterManually',
  Success = 'success',
  Error = 'error',
}
