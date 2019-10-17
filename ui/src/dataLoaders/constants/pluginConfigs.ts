// Types
import {
  TelegrafPluginName,
  TelegrafPluginInfo,
  ConfigFieldType,
  BundleName,
} from 'src/types'

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
  TelegrafPluginInputNginx,
  TelegrafPluginInputProcesses,
  TelegrafPluginInputProcstat,
  TelegrafPluginInputPrometheus,
  TelegrafPluginInputRedis,
  TelegrafPluginInputSyslog,
  TelegrafPluginInputSwap,
  TelegrafPluginInputSystem,
  TelegrafPluginInputTail,
} from '@influxdata/influx'

export const QUICKSTART_SCRAPER_TARGET_URL = `${window.location.origin}/metrics`

interface PluginBundles {
  [bundleName: string]: TelegrafPluginName[]
}

export const pluginsByBundle: PluginBundles = {
  [BundleName.System]: [
    TelegrafPluginInputCpu.NameEnum.Cpu,
    TelegrafPluginInputDisk.NameEnum.Disk,
    TelegrafPluginInputDiskio.NameEnum.Diskio,
    TelegrafPluginInputSystem.NameEnum.System,
    TelegrafPluginInputMem.NameEnum.Mem,
    TelegrafPluginInputNet.NameEnum.Net,
    TelegrafPluginInputProcesses.NameEnum.Processes,
    TelegrafPluginInputSwap.NameEnum.Swap,
  ],
  [BundleName.Docker]: [TelegrafPluginInputDocker.NameEnum.Docker],
  [BundleName.Kubernetes]: [TelegrafPluginInputKubernetes.NameEnum.Kubernetes],
  [BundleName.Nginx]: [TelegrafPluginInputNginx.NameEnum.Nginx],
  [BundleName.Redis]: [TelegrafPluginInputRedis.NameEnum.Redis],
}

export const telegrafPluginsInfo: TelegrafPluginInfo = {
  [TelegrafPluginInputCpu.NameEnum.Cpu]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputCpu.NameEnum.Cpu,
      type: TelegrafPluginInputCpu.TypeEnum.Input,
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputDisk.NameEnum.Disk]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputDisk.NameEnum.Disk,
      type: TelegrafPluginInputDisk.TypeEnum.Input,
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputDiskio.NameEnum.Diskio]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputDiskio.NameEnum.Diskio,
      type: TelegrafPluginInputDiskio.TypeEnum.Input,
    },
    templateID: '0000000000000009',
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
    templateID: '0000000000000002',
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
    templateID: '0000000000000005',
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
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputNet.NameEnum.Net]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputNet.NameEnum.Net,
      type: TelegrafPluginInputNet.TypeEnum.Input,
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputNetResponse.NameEnum.NetResponse]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputNetResponse.NameEnum.NetResponse,
      type: TelegrafPluginInputNetResponse.TypeEnum.Input,
    },
  },
  [TelegrafPluginInputNginx.NameEnum.Nginx]: {
    fields: {urls: {type: ConfigFieldType.UriArray, isRequired: true}},
    defaults: {
      name: TelegrafPluginInputNginx.NameEnum.Nginx,
      type: TelegrafPluginInputNginx.TypeEnum.Input,
    },
    templateID: '0000000000000006',
  },
  [TelegrafPluginInputProcesses.NameEnum.Processes]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputProcesses.NameEnum.Processes,
      type: TelegrafPluginInputProcesses.TypeEnum.Input,
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputProcstat.NameEnum.Procstat]: {
    fields: {exe: {type: ConfigFieldType.String, isRequired: true}},
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
    templateID: '0000000000000008',
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
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputSystem.NameEnum.System]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputSystem.NameEnum.System,
      type: TelegrafPluginInputSystem.TypeEnum.Input,
    },
    templateID: '0000000000000009',
  },
  [TelegrafPluginInputTail.NameEnum.Tail]: {
    fields: null,
    defaults: {
      name: TelegrafPluginInputTail.NameEnum.Tail,
      type: TelegrafPluginInputTail.TypeEnum.Input,
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
  TelegrafPluginInputNginx.NameEnum.Nginx,
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
  LogoRedis,
} from 'src/dataLoaders/graphics'

export const BUNDLE_LOGOS = {
  [BundleName.System]: LogoCpu,
  [BundleName.Docker]: LogoDocker,
  [BundleName.Kubernetes]: LogoKubernetes,
  [BundleName.Nginx]: LogoNginx,
  [BundleName.Redis]: LogoRedis,
}

export const PLUGIN_BUNDLE_OPTIONS: BundleName[] = [
  BundleName.System,
  BundleName.Docker,
  BundleName.Kubernetes,
  BundleName.Nginx,
  BundleName.Redis,
]
