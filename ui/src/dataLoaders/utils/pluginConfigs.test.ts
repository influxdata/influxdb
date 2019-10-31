// Utils
import {
  isPluginInBundle,
  isPluginUniqueToBundle,
} from 'src/dataLoaders/utils/pluginConfigs'

// Types
import {BundleName} from 'src/types/dataLoaders'
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputSystem,
} from '@influxdata/influx'

describe('Onboarding.Utils.PluginConfig', () => {
  describe('if plugin is found in only one bundle', () => {
    it('isPluginUniqueToBundle returns true', () => {
      const telegrafPlugin = TelegrafPluginInputCpu.NameEnum.Cpu
      const bundle = BundleName.System
      const bundles = [BundleName.System, BundleName.Docker]

      const actual = isPluginUniqueToBundle(telegrafPlugin, bundle, bundles)

      expect(actual).toBe(true)
    })
  })

  describe('if plugin is not in bundle', () => {
    it('isPluginInBundle returns false', () => {
      const telegrafPlugin = TelegrafPluginInputSystem.NameEnum.System
      const bundle = BundleName.Docker

      const actual = isPluginInBundle(telegrafPlugin, bundle)

      expect(actual).toBe(false)
    })
  })

  describe('if plugin is in bundle', () => {
    it('isPluginInBundle returns true', () => {
      const telegrafPlugin = TelegrafPluginInputSystem.NameEnum.System
      const bundle = BundleName.System

      const actual = isPluginInBundle(telegrafPlugin, bundle)

      expect(actual).toBe(true)
    })
  })
})
