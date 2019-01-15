// Reducer
import dataLoadersReducer, {
// DataLoadersState,
  INITIAL_STATE,
} from 'src/onboarding/reducers/dataLoaders'

// Actions
import {
  setDataLoadersType,
  addConfigValue,
  removeConfigValue,
  removePluginBundle,
  setActiveTelegrafPlugin,
  setTelegrafConfigID,
  updateTelegrafPluginConfig,
  addPluginBundle,
  addTelegrafPlugins,
  removeBundlePlugins,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  createOrUpdateTelegrafConfigAsync,
  setPluginConfiguration,
  setScraperTargetBucket,
  setScraperTargetURL,
  setScraperTargetID,
} from 'src/onboarding/actions/dataLoaders'

// Mock Data
import {
  redisPlugin,
  cpuTelegrafPlugin,
  diskTelegrafPlugin,
  diskioTelegrafPlugin,
  netTelegrafPlugin,
  memTelegrafPlugin,
  processesTelegrafPlugin,
  procstatTelegrafPlugin,
  systemTelegrafPlugin,
  redisTelegrafPlugin,
  token,
  telegrafConfig,
  dockerTelegrafPlugin,
} from 'mocks/dummyData'

// Types
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputDisk,
  TelegrafPluginInputRedis,
  TelegrafPluginInputKubernetes,
  TelegrafPluginInputFile,
} from 'src/api'
import {
  DataLoaderType,
  ConfigurationState,
  TelegrafPlugin,
  BundleName,
} from 'src/types/v2/dataLoaders'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))

describe('dataLoader reducer', () => {
  it('can set a type', () => {
    const actual = dataLoadersReducer(
      INITIAL_STATE,
      setDataLoadersType(DataLoaderType.Streaming)
    )
    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [],
      type: DataLoaderType.Streaming,
    }

    expect(actual).toEqual(expected)
  })

  it('can set the active telegraf plugin', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        type: DataLoaderType.Streaming,
        telegrafPlugins: [
          {
            name: TelegrafPluginInputCpu.NameEnum.Cpu,
            configured: ConfigurationState.Unconfigured,
            active: true,
          },
          {
            name: TelegrafPluginInputDisk.NameEnum.Disk,
            configured: ConfigurationState.Unconfigured,
            active: false,
          },
        ],
      },
      setActiveTelegrafPlugin(TelegrafPluginInputDisk.NameEnum.Disk)
    )

    const expected = {
      ...INITIAL_STATE,
      type: DataLoaderType.Streaming,
      telegrafPlugins: [
        {
          name: TelegrafPluginInputCpu.NameEnum.Cpu,
          configured: ConfigurationState.Unconfigured,
          active: false,
        },
        {
          name: TelegrafPluginInputDisk.NameEnum.Disk,
          configured: ConfigurationState.Unconfigured,
          active: true,
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can set the configuration state for a telegraf plugin that is not configured', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        telegrafPlugins: [
          {
            name: TelegrafPluginInputDisk.NameEnum.Disk,
            configured: ConfigurationState.Configured,
            active: false,
          },
          {
            name: TelegrafPluginInputFile.NameEnum.File,
            configured: ConfigurationState.Configured,
            active: false,
            plugin: {
              name: TelegrafPluginInputFile.NameEnum.File,
              type: TelegrafPluginInputFile.TypeEnum.Input,
              config: {
                files: [],
              },
            },
          },
        ],
      },
      setPluginConfiguration(TelegrafPluginInputFile.NameEnum.File)
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          name: TelegrafPluginInputDisk.NameEnum.Disk,
          configured: ConfigurationState.Configured,
          active: false,
        },
        {
          name: TelegrafPluginInputFile.NameEnum.File,
          configured: ConfigurationState.InvalidConfiguration,
          active: false,
          plugin: {
            name: TelegrafPluginInputFile.NameEnum.File,
            type: TelegrafPluginInputFile.TypeEnum.Input,
            config: {
              files: [],
            },
          },
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can set the configuration state for a telegraf plugin that is configured', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        telegrafPlugins: [
          {
            name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
            configured: ConfigurationState.Unconfigured,
            active: false,
            plugin: {
              name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
              type: TelegrafPluginInputKubernetes.TypeEnum.Input,
              config: {
                url: 'https://server.net',
              },
            },
          },
        ],
      },
      setPluginConfiguration(TelegrafPluginInputKubernetes.NameEnum.Kubernetes)
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
          configured: ConfigurationState.Configured,
          active: false,
          plugin: {
            name: TelegrafPluginInputKubernetes.NameEnum.Kubernetes,
            type: TelegrafPluginInputKubernetes.TypeEnum.Input,
            config: {
              url: 'https://server.net',
            },
          },
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can set a telegraf config id', () => {
    const id = '285973845720345ajfajfkl;'
    const actual = dataLoadersReducer(INITIAL_STATE, setTelegrafConfigID(id))

    const expected = {...INITIAL_STATE, telegrafConfigID: id}

    expect(actual).toEqual(expected)
  })

  it('can update a plugin config field', () => {
    const plugin = {
      ...redisPlugin,
      config: {servers: [], password: ''},
    }
    const tp: TelegrafPlugin = {
      name: TelegrafPluginInputRedis.NameEnum.Redis,
      configured: ConfigurationState.Unconfigured,
      active: true,
      plugin,
    }
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, telegrafPlugins: [tp]},
      updateTelegrafPluginConfig(
        TelegrafPluginInputRedis.NameEnum.Redis,
        'password',
        'pa$$w0rd'
      )
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          ...tp,
          plugin: {
            ...plugin,
            config: {servers: [], password: 'pa$$w0rd'},
          },
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can add a plugin config value', () => {
    const plugin = {
      ...redisPlugin,
      config: {servers: ['first'], password: ''},
    }
    const tp: TelegrafPlugin = {
      name: TelegrafPluginInputRedis.NameEnum.Redis,
      configured: ConfigurationState.Unconfigured,
      active: true,
      plugin,
    }
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, telegrafPlugins: [tp]},
      addConfigValue(
        TelegrafPluginInputRedis.NameEnum.Redis,
        'servers',
        'second'
      )
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          ...tp,
          plugin: {
            ...plugin,
            config: {servers: ['first', 'second'], password: ''},
          },
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can remove a plugin config value', () => {
    const plugin = {
      ...redisPlugin,
      config: {servers: ['first', 'second'], password: ''},
    }
    const tp: TelegrafPlugin = {
      name: TelegrafPluginInputRedis.NameEnum.Redis,
      configured: ConfigurationState.Unconfigured,
      active: true,
      plugin,
    }
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, telegrafPlugins: [tp]},
      removeConfigValue(
        TelegrafPluginInputRedis.NameEnum.Redis,
        'servers',
        'first'
      )
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          ...tp,
          plugin: {
            ...plugin,
            config: {servers: ['second'], password: ''},
          },
        },
      ],
    }
    expect(actual).toEqual(expected)
  })

  it('can add a plugin bundle', () => {
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, pluginBundles: [BundleName.Redis]},
      addPluginBundle(BundleName.System)
    )

    const expected = {
      ...INITIAL_STATE,
      pluginBundles: [BundleName.Redis, BundleName.System],
    }
    expect(actual).toEqual(expected)
  })

  it('can remove a plugin bundle', () => {
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, pluginBundles: [BundleName.Redis, BundleName.System]},
      removePluginBundle(BundleName.Redis)
    )

    const expected = {
      ...INITIAL_STATE,
      pluginBundles: [BundleName.System],
    }
    expect(actual).toEqual(expected)
  })

  it('can add telegraf Plugins', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        telegrafPlugins: [redisTelegrafPlugin, diskTelegrafPlugin],
      },
      addTelegrafPlugins([cpuTelegrafPlugin, diskTelegrafPlugin])
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        cpuTelegrafPlugin,
        diskTelegrafPlugin,
        redisTelegrafPlugin,
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can remove telegraf Plugins', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        pluginBundles: [BundleName.Docker, BundleName.System],
        telegrafPlugins: [
          cpuTelegrafPlugin,
          diskTelegrafPlugin,
          diskioTelegrafPlugin,
          netTelegrafPlugin,
          memTelegrafPlugin,
          processesTelegrafPlugin,
          procstatTelegrafPlugin,
          systemTelegrafPlugin,
          dockerTelegrafPlugin,
        ],
      },
      removeBundlePlugins(BundleName.System)
    )

    const expected = {
      ...INITIAL_STATE,
      pluginBundles: [BundleName.Docker, BundleName.System],
      telegrafPlugins: [dockerTelegrafPlugin],
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget bucket', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
      },
      setScraperTargetBucket('a')
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: 'a', url: ''},
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget url', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        scraperTarget: {bucket: '', url: ''},
      },
      setScraperTargetURL('http://googledoodle.com')
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: '', url: 'http://googledoodle.com'},
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget url', () => {
    const id = '12345678'
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        scraperTarget: {bucket: '', url: ''},
      },
      setScraperTargetID(id)
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: '', url: '', id},
    }

    expect(actual).toEqual(expected)
  })

  // ---------- Thunks ------------ //

  it('can create a telegraf config', async () => {
    const dispatch = jest.fn()
    const org = 'default'
    const bucket = 'defbuck'
    const telegrafPlugins = [cpuTelegrafPlugin]
    const getState = (): any => ({
      onboarding: {
        dataLoaders: {
          telegrafPlugins,
        },
        steps: {
          setupParams: {org, bucket},
        },
      },
    })
    await createOrUpdateTelegrafConfigAsync(token)(dispatch, getState)

    expect(dispatch).toBeCalledWith(setTelegrafConfigID(telegrafConfig.id))
  })

  it('can add a plugin bundle with plugins', () => {
    const dispatch = jest.fn()
    addPluginBundleWithPlugins(BundleName.System)(dispatch)

    expect(dispatch).toBeCalledWith(addPluginBundle(BundleName.System))
    expect(dispatch).toBeCalledWith(
      addTelegrafPlugins([
        cpuTelegrafPlugin,
        diskTelegrafPlugin,
        diskioTelegrafPlugin,
        systemTelegrafPlugin,
        memTelegrafPlugin,
        netTelegrafPlugin,
        processesTelegrafPlugin,
        procstatTelegrafPlugin,
      ])
    )
  })

  it('can remove a plugin bundle and its plugins', () => {
    const dispatch = jest.fn()
    removePluginBundleWithPlugins(BundleName.System)(dispatch)

    expect(dispatch).toBeCalledWith(removePluginBundle(BundleName.System))
    expect(dispatch).toBeCalledWith(removeBundlePlugins(BundleName.System))
  })
})
