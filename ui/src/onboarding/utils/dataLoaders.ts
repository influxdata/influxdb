// Types
import {DataLoaderType, ConfigurationState} from 'src/types/v2/dataSources'

export const getInitialDataSources = (type: DataLoaderType) => {
  if (type === DataLoaderType.Streaming) {
    return []
  }

  return [
    {
      name: type,
      configured: ConfigurationState.Unconfigured,
      active: true,
      configs: null,
    },
  ]
}
