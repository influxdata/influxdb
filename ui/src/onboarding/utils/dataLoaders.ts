// Types
import {DataSourceType, ConfigurationState} from 'src/types/v2/dataSources'

export const getInitialDataSources = (type: DataSourceType) => {
  if (type === DataSourceType.Streaming) {
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
