// Types
import {
  TelegrafPluginName,
  TelegrafPluginInfo,
  ConfigFieldType,
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
    fields: {endpoint: ConfigFieldType.String},
    defaults: {
      name: TelegrafPluginInputDocker.NameEnum.Docker,
      type: TelegrafPluginInputDocker.TypeEnum.Input,
      config: {endpoint: ''},
    },
  },
  [TelegrafPluginInputFile.NameEnum.File]: {
    fields: {files: ConfigFieldType.StringArray},
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
    fields: {url: ConfigFieldType.Uri},
    defaults: {
      name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
      type: TelegrafPluginInputKubernetes.TypeEnum.Input,
      config: {url: ''},
    },
  },
  [TelegrafPluginInputLogParser.NameEnum.Logparser]: {
    fields: {files: ConfigFieldType.StringArray},
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
    fields: {exe: ConfigFieldType.String},
    defaults: {
      name: TelegrafPluginInputProcstat.NameEnum.Procstat,
      type: TelegrafPluginInputProcstat.TypeEnum.Input,
      config: {exe: ''},
    },
  },
  [TelegrafPluginInputPrometheus.NameEnum.Prometheus]: {
    fields: {urls: ConfigFieldType.UriArray},
    defaults: {
      name: TelegrafPluginInputPrometheus.NameEnum.Prometheus,
      type: TelegrafPluginInputPrometheus.TypeEnum.Input,
      config: {urls: []},
    },
  },
  [TelegrafPluginInputRedis.NameEnum.Redis]: {
    fields: {
      servers: ConfigFieldType.StringArray,
      password: ConfigFieldType.String,
    },
    defaults: {
      name: TelegrafPluginInputRedis.NameEnum.Redis,
      type: TelegrafPluginInputRedis.TypeEnum.Input,
      config: {servers: [], password: ''},
    },
  },
  [TelegrafPluginInputSyslog.NameEnum.Syslog]: {
    fields: {server: ConfigFieldType.String},
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

export const PLUGIN_LOGOS = {
  [TelegrafPluginInputCpu.NameEnum.Cpu]: LogoCpu,
  [TelegrafPluginInputDocker.NameEnum.Docker]: LogoDocker,
  [TelegrafPluginInputKubernetes.NameEnum.Kubernetes]: LogoKubernetes,
  [TelegrafPluginInputNgnix.NameEnum.Ngnix]: LogoNginx,
  [TelegrafPluginInputPrometheus.NameEnum.Prometheus]: LogoPrometheus,
  [TelegrafPluginInputRedis.NameEnum.Redis]: LogoRedis,
}
