// Utils
import {
  isPluginInBundle,
  isPluginUniqueToBundle,
} from 'src/onboarding/utils/pluginConfigs'

// Types
import {BundleName} from 'src/types/v2/dataLoaders'
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputDisk,
  TelegrafPluginInputSystem,
} from 'src/api'

describe('Onboarding.Utils.PluginConfig', () => {
  describe('if plugin is found in only one bundle', () => {
    it('isPluginUniqueToBundle returns true', () => {
      const telegrafPlugin = TelegrafPluginInputCpu.NameEnum.Cpu
      const bundle = BundleName.System
      const bundles = [BundleName.System, BundleName.Disk]

      const actual = isPluginUniqueToBundle(telegrafPlugin, bundle, bundles)

      expect(actual).toBe(true)
    })
  })
  describe('if plugin is found in multiple bundles', () => {
    it('isPluginUniqueToBundle returns false', () => {
      const telegrafPlugin = TelegrafPluginInputDisk.NameEnum.Disk
      const bundle = BundleName.System
      const bundles = [BundleName.System, BundleName.Disk]

      const actual = isPluginUniqueToBundle(telegrafPlugin, bundle, bundles)

      expect(actual).toBe(false)
    })
  })
  describe('if plugin is not in bundle', () => {
    it('isPluginInBundle returns false', () => {
      const telegrafPlugin = TelegrafPluginInputSystem.NameEnum.System
      const bundle = BundleName.Disk

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
