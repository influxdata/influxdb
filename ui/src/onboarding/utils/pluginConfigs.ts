// Constants
import {
  telegrafPluginsInfo,
  pluginsByBundle,
} from 'src/onboarding/constants/pluginConfigs'

// Types
import {
  TelegrafPluginName,
  ConfigFields,
  Plugin,
  BundleName,
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

export const isPluginUniqueToBundle = (
  telegrafPlugin: TelegrafPluginName,
  bundle: BundleName,
  bundles: BundleName[]
): boolean => {
  return bundles.reduce((acc, b) => {
    if (b === bundle) {
      return acc
    }

    pluginsByBundle[b].forEach(p => {
      if (p === telegrafPlugin) {
        acc = false
      }
    })

    return acc
  }, true)
}

export const isPluginInBundle = (
  telegrafPlugin: TelegrafPluginName,
  bundle: BundleName
) => {
  return !!pluginsByBundle[bundle].find(p => p === telegrafPlugin)
}
