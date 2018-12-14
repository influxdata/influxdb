// Types
import {
  TelegrafPluginName,
  TelegrafPluginInfo,
  ConfigFieldType,
  BundleName,
} from 'src/types/v2/dataLoaders'
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
} from 'src/api'

interface PluginBundles {
  [bundleName: string]: TelegrafPluginName[]
}

export const pluginsByBundle: PluginBundles = {
  [BundleName.System]: [
    TelegrafPluginInputCpu.NameEnum.Cpu,
    TelegrafPluginInputDisk.NameEnum.Disk,
    TelegrafPluginInputDiskio.NameEnum.Diskio,
    TelegrafPluginInputKernel.NameEnum.Kernel,
    TelegrafPluginInputMem.NameEnum.Mem,
    TelegrafPluginInputProcesses.NameEnum.Processes,
    TelegrafPluginInputSwap.NameEnum.Swap,
    TelegrafPluginInputSystem.NameEnum.System,
  ],
  [BundleName.Disk]: [
    TelegrafPluginInputDisk.NameEnum.Disk,
    TelegrafPluginInputDiskio.NameEnum.Diskio,
  ],
  [BundleName.Docker]: [TelegrafPluginInputDocker.NameEnum.Docker],
  [BundleName.File]: [TelegrafPluginInputFile.NameEnum.File],
  [BundleName.Kubernetes]: [TelegrafPluginInputKubernetes.NameEnum.Kubernetes],
  [BundleName.Logparser]: [TelegrafPluginInputLogParser.NameEnum.Logparser],
  [BundleName.Net]: [TelegrafPluginInputNet.NameEnum.Net],
  [BundleName.NetResponse]: [
    TelegrafPluginInputNetResponse.NameEnum.NetResponse,
  ],
  [BundleName.Ngnix]: [TelegrafPluginInputNgnix.NameEnum.Ngnix],
  [BundleName.Procstat]: [TelegrafPluginInputProcstat.NameEnum.Procstat],
  [BundleName.Prometheus]: [TelegrafPluginInputPrometheus.NameEnum.Prometheus],
  [BundleName.Redis]: [TelegrafPluginInputRedis.NameEnum.Redis],
  [BundleName.Syslog]: [TelegrafPluginInputSyslog.NameEnum.Syslog],
  [BundleName.Tail]: [TelegrafPluginInputTail.NameEnum.Tail],
}

export const telegrafPluginsInfo: TelegrafPluginInfo = {
  [TelegrafPluginInputCpu.NameEnum.Cpu]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputCpu.NameEnum.Cpu,
      type: TelegrafPluginInputCpu.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputDisk.NameEnum.Disk]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputDisk.NameEnum.Disk,
      type: TelegrafPluginInputDisk.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputDiskio.NameEnum.Diskio]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputDiskio.NameEnum.Diskio,
      type: TelegrafPluginInputDiskio.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputDocker.NameEnum.Docker]: {
    fields: {
      endpoint: {
        type: ConfigFieldType.String,
        isRequired: true,
      },
    },
    defaults: {
      name: TelegrafPluginInputDocker.NameEnum.Docker,
      type: TelegrafPluginInputDocker.TypeEnum.Input,
      config: {endpoint: ''},
    },
  },
  [TelegrafPluginInputFile.NameEnum.File]: {
    fields: {
      files: {
        type: ConfigFieldType.StringArray,
        isRequired: true,
      },
    },
    defaults: {
      name: TelegrafPluginInputFile.NameEnum.File,
      type: TelegrafPluginInputFile.TypeEnum.Input,
      config: {files: []},
    },
  },
  [TelegrafPluginInputKernel.NameEnum.Kernel]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputKernel.NameEnum.Kernel,
      type: TelegrafPluginInputDiskio.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputKubernetes.NameEnum.Kubernetes]: {
    fields: {
      url: {
        type: ConfigFieldType.Uri,
        isRequired: true,
      },
    },
    defaults: {
      name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
      type: TelegrafPluginInputKubernetes.TypeEnum.Input,
      config: {url: ''},
    },
  },
  [TelegrafPluginInputLogParser.NameEnum.Logparser]: {
    fields: {files: {type: ConfigFieldType.StringArray, isRequired: true}},
    defaults: {
      name: TelegrafPluginInputLogParser.NameEnum.Logparser,
      type: TelegrafPluginInputLogParser.TypeEnum.Input,
      config: {files: []},
    },
  },
  [TelegrafPluginInputMem.NameEnum.Mem]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputMem.NameEnum.Mem,
      type: TelegrafPluginInputMem.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputNet.NameEnum.Net]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputNet.NameEnum.Net,
      type: TelegrafPluginInputNet.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputNetResponse.NameEnum.NetResponse]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputNetResponse.NameEnum.NetResponse,
      type: TelegrafPluginInputNetResponse.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputNgnix.NameEnum.Ngnix]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputNgnix.NameEnum.Ngnix,
      type: TelegrafPluginInputNgnix.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputProcesses.NameEnum.Processes]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputProcesses.NameEnum.Processes,
      type: TelegrafPluginInputProcesses.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputProcstat.NameEnum.Procstat]: {
    fields: {exe: {type: ConfigFieldType.String, isRequired: false}},
    defaults: {
      name: TelegrafPluginInputProcstat.NameEnum.Procstat,
      type: TelegrafPluginInputProcstat.TypeEnum.Input,
      config: {exe: ''},
    },
  },
  [TelegrafPluginInputPrometheus.NameEnum.Prometheus]: {
    fields: {urls: {type: ConfigFieldType.UriArray, isRequired: true}},
    defaults: {
      name: TelegrafPluginInputPrometheus.NameEnum.Prometheus,
      type: TelegrafPluginInputPrometheus.TypeEnum.Input,
      config: {urls: []},
    },
  },
  [TelegrafPluginInputRedis.NameEnum.Redis]: {
    fields: {
      servers: {type: ConfigFieldType.StringArray, isRequired: true},
      password: {type: ConfigFieldType.String, isRequired: false},
    },
    defaults: {
      name: TelegrafPluginInputRedis.NameEnum.Redis,
      type: TelegrafPluginInputRedis.TypeEnum.Input,
      config: {servers: [], password: ''},
    },
  },
  [TelegrafPluginInputSyslog.NameEnum.Syslog]: {
    fields: {server: {type: ConfigFieldType.String, isRequired: true}},
    defaults: {
      name: TelegrafPluginInputSyslog.NameEnum.Syslog,
      type: TelegrafPluginInputSyslog.TypeEnum.Input,
      config: {server: ''},
    },
  },
  [TelegrafPluginInputSwap.NameEnum.Swap]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputSwap.NameEnum.Swap,
      type: TelegrafPluginInputSwap.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputSystem.NameEnum.System]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputSystem.NameEnum.System,
      type: TelegrafPluginInputSystem.TypeEnum.Input,
      config: {},
    },
  },
  [TelegrafPluginInputTail.NameEnum.Tail]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputTail.NameEnum.Tail,
      type: TelegrafPluginInputTail.TypeEnum.Input,
      config: {},
    },
  },
}

export const PLUGIN_OPTIONS: TelegrafPluginName[] = [
  TelegrafPluginInputCpu.NameEnum.Cpu,
  TelegrafPluginInputDisk.NameEnum.Disk,
  TelegrafPluginInputDiskio.NameEnum.Diskio,
  TelegrafPluginInputDocker.NameEnum.Docker,
  TelegrafPluginInputFile.NameEnum.File,
  TelegrafPluginInputKernel.NameEnum.Kernel,
  TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
  TelegrafPluginInputLogParser.NameEnum.Logparser,
  TelegrafPluginInputMem.NameEnum.Mem,
  TelegrafPluginInputNet.NameEnum.Net,
  TelegrafPluginInputNetResponse.NameEnum.NetResponse,
  TelegrafPluginInputNgnix.NameEnum.Ngnix,
  TelegrafPluginInputProcesses.NameEnum.Processes,
  TelegrafPluginInputProcstat.NameEnum.Procstat,
  TelegrafPluginInputPrometheus.NameEnum.Prometheus,
  TelegrafPluginInputRedis.NameEnum.Redis,
  TelegrafPluginInputSyslog.NameEnum.Syslog,
  TelegrafPluginInputSwap.NameEnum.Swap,
  TelegrafPluginInputSystem.NameEnum.System,
  TelegrafPluginInputTail.NameEnum.Tail,
]

import {
  LogoCpu,
  LogoDocker,
  LogoKubernetes,
  LogoNginx,
  LogoPrometheus,
  LogoRedis,
} from 'src/onboarding/graphics'

export const BUNDLE_LOGOS = {
  [BundleName.System]: LogoCpu,
  [BundleName.Docker]: LogoDocker,
  [BundleName.Kubernetes]: LogoKubernetes,
  [BundleName.Ngnix]: LogoNginx,
  [BundleName.Prometheus]: LogoPrometheus,
  [BundleName.Redis]: LogoRedis,
}

export const PLUGIN_BUNDLE_OPTIONS: BundleName[] = [
  BundleName.System,
  BundleName.Disk,
  BundleName.Docker,
  BundleName.File,
  BundleName.Kubernetes,
  BundleName.Logparser,
  BundleName.Net,
  BundleName.NetResponse,
  BundleName.Ngnix,
  BundleName.Procstat,
  BundleName.Prometheus,
  BundleName.Redis,
  BundleName.Syslog,
  BundleName.Tail,
]
