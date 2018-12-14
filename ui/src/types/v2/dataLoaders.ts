// Types
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputDisk,
  TelegrafPluginInputDiskio,
  TelegrafPluginInputDocker,
  TelegrafPluginInputFile,
  TelegrafPluginInputKernel,
  TelegrafPluginInputKubernetes,
  TelegrafPluginInputLogParser,
  TelegrafPluginInputMem,
  TelegrafPluginInputNet,
  TelegrafPluginInputNetResponse,
  TelegrafPluginInputNgnix,
  TelegrafPluginInputProcesses,
  TelegrafPluginInputProcstat,
  TelegrafPluginInputPrometheus,
  TelegrafPluginInputRedis,
  TelegrafPluginInputSyslog,
  TelegrafPluginInputSwap,
  TelegrafPluginInputSystem,
  TelegrafPluginInputTail,
  TelegrafPluginOutputFile,
  TelegrafPluginOutputInfluxDBV2,
  TelegrafPluginConfig,
  TelegrafPluginInputDockerConfig,
  TelegrafPluginInputFileConfig,
  TelegrafPluginInputKubernetesConfig,
  TelegrafPluginInputLogParserConfig,
  TelegrafPluginInputProcstatConfig,
  TelegrafPluginInputPrometheusConfig,
  TelegrafPluginInputRedisConfig,
  TelegrafPluginInputSyslogConfig,
  TelegrafPluginOutputFileConfig,
  TelegrafPluginOutputInfluxDBV2Config,
} from 'src/api'
import {RemoteDataState} from 'src/types'
import {WritePrecision} from 'src/api'

export interface DataLoadersState {
  telegrafPlugins: TelegrafPlugin[]
  pluginBundles: BundleName[]
  type: DataLoaderType
  activeLPTab: LineProtocolTab
  telegrafConfigID: string
  lpStatus: RemoteDataState
  lineProtocolBody: string
  precision: WritePrecision
}

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

export type PluginConfig =
  | TelegrafPluginConfig
  | TelegrafPluginInputDockerConfig
  | TelegrafPluginInputFileConfig
  | TelegrafPluginInputKubernetesConfig
  | TelegrafPluginInputLogParserConfig
  | TelegrafPluginInputProcstatConfig
  | TelegrafPluginInputPrometheusConfig
  | TelegrafPluginInputRedisConfig
  | TelegrafPluginInputSyslogConfig
  | TelegrafPluginOutputFileConfig
  | TelegrafPluginOutputInfluxDBV2Config

export type Plugin =
  | TelegrafPluginInputCpu
  | TelegrafPluginInputDisk
  | TelegrafPluginInputDiskio
  | TelegrafPluginInputDocker
  | TelegrafPluginInputFile
  | TelegrafPluginInputKernel
  | TelegrafPluginInputKubernetes
  | TelegrafPluginInputLogParser
  | TelegrafPluginInputMem
  | TelegrafPluginInputNet
  | TelegrafPluginInputNetResponse
  | TelegrafPluginInputNgnix
  | TelegrafPluginInputProcesses
  | TelegrafPluginInputProcstat
  | TelegrafPluginInputPrometheus
  | TelegrafPluginInputRedis
  | TelegrafPluginInputSyslog
  | TelegrafPluginInputSwap
  | TelegrafPluginInputSystem
  | TelegrafPluginInputTail
  | TelegrafPluginOutputFile
  | TelegrafPluginOutputInfluxDBV2

export interface TelegrafPlugin {
  name: TelegrafPluginName
  configured: ConfigurationState
  active: boolean
  plugin?: Plugin
}

export enum BundleName {
  System = 'System',
  Disk = 'Disk',
  Docker = 'Docker',
  File = 'File',
  Kubernetes = 'Kubernetes',
  Logparser = 'LogParser',
  Net = 'Net',
  NetResponse = 'NetResponse',
  Ngnix = 'NGNIX',
  Procstat = 'Procstat',
  Prometheus = 'Prometheus',
  Redis = 'Redis',
  Syslog = 'Syslog',
  Tail = 'Tail',
}

export type TelegrafPluginName =
  | TelegrafPluginInputCpu.NameEnum.Cpu
  | TelegrafPluginInputDisk.NameEnum.Disk
  | TelegrafPluginInputDiskio.NameEnum.Diskio
  | TelegrafPluginInputDocker.NameEnum.Docker
  | TelegrafPluginInputFile.NameEnum.File
  | TelegrafPluginInputKernel.NameEnum.Kernel
  | TelegrafPluginInputKubernetes.NameEnum.Kubernetes
  | TelegrafPluginInputLogParser.NameEnum.Logparser
  | TelegrafPluginInputMem.NameEnum.Mem
  | TelegrafPluginInputNet.NameEnum.Net
  | TelegrafPluginInputNetResponse.NameEnum.NetResponse
  | TelegrafPluginInputNgnix.NameEnum.Ngnix
  | TelegrafPluginInputProcesses.NameEnum.Processes
  | TelegrafPluginInputProcstat.NameEnum.Procstat
  | TelegrafPluginInputPrometheus.NameEnum.Prometheus
  | TelegrafPluginInputRedis.NameEnum.Redis
  | TelegrafPluginInputSyslog.NameEnum.Syslog
  | TelegrafPluginInputSwap.NameEnum.Swap
  | TelegrafPluginInputSystem.NameEnum.System
  | TelegrafPluginInputTail.NameEnum.Tail
  | TelegrafPluginOutputFile.NameEnum.File
  | TelegrafPluginOutputInfluxDBV2.NameEnum.InfluxdbV2

export enum LineProtocolTab {
  UploadFile = 'uploadFile',
  EnterManually = 'enterManually',
  EnterURL = 'enterURL',
}

export enum LineProtocolStatus {
  ImportData = 'importData',
  Loading = 'loading',
  Success = 'success',
  Error = 'error',
}

export enum Precision {
  Milliseconds = 'Milliseconds',
  Seconds = 'Seconds',
  Microseconds = 'Microseconds',
  U = 'U',
  Nanoseconds = 'Nanoseconds',
}

export enum ConfigFieldType {
  String = 'string',
  StringArray = 'string array',
  Uri = 'uri',
  UriArray = 'uri array',
}

export interface ConfigFields {
  [field: string]: {
    type: ConfigFieldType
    isRequired: boolean
  }
}

export interface TelegrafPluginInfo {
  [name: string]: {
    fields: ConfigFields
    defaults: Plugin
  }
}
