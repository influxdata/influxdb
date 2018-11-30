// Reducer
import dataLoadersReducer, {
// DataLoadersState,
  INITIAL_STATE,
} from 'src/onboarding/reducers/dataLoaders'

// Actions
import {
  setDataLoadersType,
  addDataSource,
  removeDataSource,
} from 'src/onboarding/actions/dataLoaders'

import {DataLoaderType, ConfigurationState} from 'src/types/v2/dataLoaders'

describe('dataLoader reducer', () => {
  describe('if type is streaming', () => {
    it('can set a type', () => {
      const actual = dataLoadersReducer(
        INITIAL_STATE,
        setDataLoadersType(DataLoaderType.Streaming)
      )
      const expected = {dataSources: [], type: DataLoaderType.Streaming}

      expect(actual).toEqual(expected)
    })
  })

  describe('if type is not streaming', () => {
    it('cant set a type not streaming', () => {
      const actual = dataLoadersReducer(
        INITIAL_STATE,
        setDataLoadersType(DataLoaderType.CSV)
      )
      const expected = {
        dataSources: [
          {
            name: 'CSV',
            configured: ConfigurationState.Unconfigured,
            active: true,
            configs: null,
          },
        ],
        type: DataLoaderType.CSV,
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('if data source is added', () => {
    it('can add a data source', () => {
      const actual = dataLoadersReducer(
        INITIAL_STATE,
        addDataSource({
          name: 'CSV',
          configured: ConfigurationState.Unconfigured,
          active: true,
          configs: null,
        })
      )
      const expected = {
        dataSources: [
          {
            name: 'CSV',
            configured: ConfigurationState.Unconfigured,
            active: true,
            configs: null,
          },
        ],
        type: DataLoaderType.Empty,
      }

      expect(actual).toEqual(expected)
    })
  })

  it('can add a streaming data source', () => {
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, type: DataLoaderType.Streaming},
      addDataSource({
        name: 'CPU',
        configured: ConfigurationState.Unconfigured,
        active: true,
        configs: null,
      })
    )
    const expected = {
      dataSources: [
        {
          name: 'CPU',
          configured: ConfigurationState.Unconfigured,
          active: true,
          configs: null,
        },
      ],
      type: DataLoaderType.Streaming,
    }

    expect(actual).toEqual(expected)
  })

  it('can remove a streaming data source', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        type: DataLoaderType.Streaming,
        dataSources: [
          {
            name: 'CPU',
            configured: ConfigurationState.Unconfigured,
            active: true,
            configs: null,
          },
        ],
      },
      removeDataSource('CPU')
    )
    const expected = {
      dataSources: [],
      type: DataLoaderType.Streaming,
    }

    expect(actual).toEqual(expected)
  })
})
