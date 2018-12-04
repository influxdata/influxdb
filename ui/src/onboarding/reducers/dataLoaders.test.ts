// Reducer
import dataLoadersReducer, {
// DataLoadersState,
  INITIAL_STATE,
} from 'src/onboarding/reducers/dataLoaders'

// Actions
import {
  setDataLoadersType,
  addTelegrafPlugin,
  removeTelegrafPlugin,
  setActiveTelegrafPlugin,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafRequestPlugins} from 'src/api'
import {DataLoaderType, ConfigurationState} from 'src/types/v2/dataLoaders'

describe('dataLoader reducer', () => {
  it('can set a type', () => {
    const actual = dataLoadersReducer(
      INITIAL_STATE,
      setDataLoadersType(DataLoaderType.Streaming)
    )
    const expected = {telegrafPlugins: [], type: DataLoaderType.Streaming}

    expect(actual).toEqual(expected)
  })

  it('can add a telegraf plugin', () => {
    const actual = dataLoadersReducer(
      INITIAL_STATE,
      addTelegrafPlugin({
        name: TelegrafRequestPlugins.NameEnum.Cpu,
        configured: ConfigurationState.Unconfigured,
        active: true,
        config: null,
      })
    )
    const expected = {
      telegrafPlugins: [
        {
          name: TelegrafRequestPlugins.NameEnum.Cpu,
          configured: ConfigurationState.Unconfigured,
          active: true,
          config: null,
        },
      ],
      type: DataLoaderType.Empty,
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
            name: TelegrafRequestPlugins.NameEnum.Cpu,
            configured: ConfigurationState.Unconfigured,
            active: true,
            config: null,
          },
          {
            name: TelegrafRequestPlugins.NameEnum.Disk,
            configured: ConfigurationState.Unconfigured,
            active: false,
            config: null,
          },
        ],
      },
      setActiveTelegrafPlugin(TelegrafRequestPlugins.NameEnum.Disk)
    )

    const expected = {
      type: DataLoaderType.Streaming,
      telegrafPlugins: [
        {
          name: TelegrafRequestPlugins.NameEnum.Cpu,
          configured: ConfigurationState.Unconfigured,
          active: false,
          config: null,
        },
        {
          name: TelegrafRequestPlugins.NameEnum.Disk,
          configured: ConfigurationState.Unconfigured,
          active: true,
          config: null,
        },
      ],
    }

    expect(actual).toEqual(expected)
  })

  it('can remove a telegraf plugin', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        type: DataLoaderType.Streaming,
        telegrafPlugins: [
          {
            name: TelegrafRequestPlugins.NameEnum.Cpu,
            configured: ConfigurationState.Unconfigured,
            active: true,
            config: null,
          },
        ],
      },
      removeTelegrafPlugin(TelegrafRequestPlugins.NameEnum.Cpu)
    )
    const expected = {
      telegrafPlugins: [],
      type: DataLoaderType.Streaming,
    }

    expect(actual).toEqual(expected)
  })
})
