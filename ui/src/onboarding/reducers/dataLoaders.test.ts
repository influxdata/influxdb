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
  setTelegrafConfigID,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPluginInputCpu, TelegrafPluginInputDisk} from 'src/api'
import {DataLoaderType, ConfigurationState} from 'src/types/v2/dataLoaders'

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

  it('can add a telegraf plugin', () => {
    const actual = dataLoadersReducer(
      INITIAL_STATE,
      addTelegrafPlugin({
        name: TelegrafPluginInputCpu.NameEnum.Cpu,
        configured: ConfigurationState.Unconfigured,
        active: true,
      })
    )
    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [
        {
          name: TelegrafPluginInputCpu.NameEnum.Cpu,
          configured: ConfigurationState.Unconfigured,
          active: true,
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

  it('can remove a telegraf plugin', () => {
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
        ],
      },
      removeTelegrafPlugin(TelegrafPluginInputCpu.NameEnum.Cpu)
    )
    const expected = {
      ...INITIAL_STATE,
      telegrafPlugins: [],
      type: DataLoaderType.Streaming,
    }

    expect(actual).toEqual(expected)
  })

  it('can set a telegraf config id', () => {
    const id = '285973845720345ajfajfkl;'
    const actual = dataLoadersReducer(INITIAL_STATE, setTelegrafConfigID(id))

    const expected = {...INITIAL_STATE, telegrafConfigID: id}

    expect(actual).toEqual(expected)
  })
})
