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

import {DataSourceType, ConfigurationState} from 'src/types/v2/dataSources'

describe('dataLoader reducer', () => {
  describe('if type is streaming', () => {
    it('can set a type', () => {
      const actual = dataLoadersReducer(
        INITIAL_STATE,
        setDataLoadersType(DataSourceType.Streaming)
      )
      const expected = {dataSources: [], type: DataSourceType.Streaming}

      expect(actual).toEqual(expected)
    })
  })

  describe('if type is not streaming', () => {
    it('cant set a type not streaming', () => {
      const actual = dataLoadersReducer(
        INITIAL_STATE,
        setDataLoadersType(DataSourceType.CSV)
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
        type: DataSourceType.CSV,
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
        type: DataSourceType.Empty,
      }

      expect(actual).toEqual(expected)
    })
  })

  it('can add a streaming data source', () => {
    const actual = dataLoadersReducer(
      {...INITIAL_STATE, type: DataSourceType.Streaming},
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
      type: DataSourceType.Streaming,
    }

    expect(actual).toEqual(expected)
  })

  it('can remove a streaming data source', () => {
    const actual = dataLoadersReducer(
      {
        ...INITIAL_STATE,
        type: DataSourceType.Streaming,
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
      type: DataSourceType.Streaming,
    }

    expect(actual).toEqual(expected)
  })
})
