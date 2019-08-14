// Reducer
import dataLoadersReducer, {
  INITIAL_STATE,
} from 'src/dataLoaders/reducers/dataLoaders'

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
  setTelegrafConfigName,
  setScraperTargetName,
} from 'src/dataLoaders/actions/dataLoaders'

// Mock Data
import {
  redisPlugin,
  cpuTelegrafPlugin,
  diskTelegrafPlugin,
  diskioTelegrafPlugin,
  netTelegrafPlugin,
  memTelegrafPlugin,
  processesTelegrafPlugin,
  systemTelegrafPlugin,
  redisTelegrafPlugin,
  telegrafConfig,
  dockerTelegrafPlugin,
  swapTelegrafPlugin,
} from 'mocks/dummyData'
import {QUICKSTART_SCRAPER_TARGET_URL} from 'src/dataLoaders/constants/pluginConfigs'

// Types
import {
  TelegrafPluginInputCpu,
  TelegrafPluginInputDisk,
  TelegrafPluginInputRedis,
  TelegrafPluginInputKubernetes,
  TelegrafPluginInputFile,
} from '@influxdata/influx'
import {
  DataLoaderType,
  ConfigurationState,
  TelegrafPlugin,
  BundleName,
} from 'src/types/dataLoaders'

jest.mock('src/utils/api', () => require('src/onboarding/apis/mocks'))
jest.mock('src/authorizations/apis')

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
            templateID: '0000000000000009',
          },
          {
            name: TelegrafPluginInputDisk.NameEnum.Disk,
            configured: ConfigurationState.Unconfigured,
            active: false,
            templateID: '0000000000000009',
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
          templateID: '0000000000000009',
        },
        {
          name: TelegrafPluginInputDisk.NameEnum.Disk,
          configured: ConfigurationState.Unconfigured,
          active: true,
          templateID: '0000000000000009',
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
            templateID: '0000000000000009',
          },
          {
            name: TelegrafPluginInputFile.NameEnum.File,
            configured: ConfigurationState.Configured,
            active: false,
            templateID: '0000000000000009',
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
          templateID: '0000000000000009',
        },
        {
          name: TelegrafPluginInputFile.NameEnum.File,
          configured: ConfigurationState.InvalidConfiguration,
          active: false,
          templateID: '0000000000000009',
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
            templateID: '0000000000000005',
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
          templateID: '0000000000000005',
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

  it('can set telegraf config name ', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
      },
      setTelegrafConfigName('myConfig')
    )

    const expected = {
      ...INITIAL_STATE,
      telegrafConfigName: 'myConfig',
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
      scraperTarget: {
        bucket: 'a',
        url: QUICKSTART_SCRAPER_TARGET_URL,
        name: 'Name this Scraper Target',
      },
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget url', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        scraperTarget: {bucket: '', url: '', name: ''},
      },
      setScraperTargetURL('http://googledoodle.com')
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: '', url: 'http://googledoodle.com', name: ''},
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget id', () => {
    const id = '12345678'
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        scraperTarget: {bucket: '', url: '', name: ''},
      },
      setScraperTargetID(id)
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: '', url: '', name: '', id},
    }

    expect(actual).toEqual(expected)
  })

  it('can set scraperTarget name', () => {
    const name = 'MyTarget'
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        scraperTarget: {bucket: '', url: '', name: ''},
      },
      setScraperTargetName(name)
    )

    const expected = {
      ...INITIAL_STATE,
      scraperTarget: {bucket: '', url: '', name},
    }

    expect(actual).toEqual(expected)
  })

  // ---------- Thunks ------------ //

  it.skip('can create a telegraf config', async () => {
    const dispatch = jest.fn()
    const org = 'default'
    const bucket = 'defbuck'
    const telegrafPlugins = [cpuTelegrafPlugin]
    const getState = (): any => ({
      dataLoading: {
        dataLoaders: {
          telegrafPlugins,
        },
        steps: {
          org,
          bucket,
        },
      },
    })

    await createOrUpdateTelegrafConfigAsync()(dispatch, getState)

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
        swapTelegrafPlugin,
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
