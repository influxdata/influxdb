// Constants
import {telegrafPluginsInfo} from 'src/onboarding/constants/pluginConfigs'

// Types
import {
  TelegrafPluginName,
  ConfigFields,
  Plugin,
} from 'src/types/v2/dataLoaders'

export const getConfigFields = (
  pluginName: TelegrafPluginName
): ConfigFields => {
  return telegrafPluginsInfo[pluginName].fields
}

export const updateConfigFields = <T extends Plugin>(
  plugin: T,
  fieldName: string,
  value: string[] | string
): T => {
  return Object.assign({}, plugin, {
    config: Object.assign({}, plugin.config, {[fieldName]: value}),
  })
}

export const createNewPlugin = (name: TelegrafPluginName): Plugin => {
  return telegrafPluginsInfo[name].defaults
}
