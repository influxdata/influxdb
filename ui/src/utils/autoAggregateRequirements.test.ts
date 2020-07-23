import applyAutoAggregateRequirements from 'src/utils/autoAggregateRequirements'
import {hasQueryBeenEdited} from 'src/timeMachine/utils/queryBuilder'

import {cloneDeep} from 'lodash'
import {
  AGG_WINDOW_AUTO,
  DEFAULT_FILLVALUES,
} from 'src/timeMachine/constants/queryBuilder'

const autoAggregateableBuilderConfig = {
  properties: {
    queries: [
      {
        text:
          'from(bucket: "asdf")\n  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)\n  |> filter(fn: (r) => r["_measurement"] == "cpu")\n  |> filter(fn: (r) => r["_field"] == "usage_user")\n  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: true)\n  |> yield(name: "mean")',
        editMode: 'builder' as 'builder',
        builderConfig: {
          buckets: ['asdf'],
          tags: [
            {
              key: '_measurement',
              values: ['cpu'],
              aggregateFunctionType: 'filter',
            },
            {
              key: '_field',
              values: ['usage_user'],
              aggregateFunctionType: 'filter',
            },
            {
              key: 'cpu',
              values: [],
              aggregateFunctionType: 'filter',
            },
          ],
          functions: [
            {
              name: 'mean',
            },
          ],
          aggregateWindow: {
            period: 'auto',
            fillValues: false,
          },
        },
      },
    ],
  },
} as any

describe('applyAutoAggregateRequirements', () => {
  it('does not alter advanced queries', () => {
    const advancedModeQuery = {
      properties: {
        queries: [
          {
            text: 'something',
            editMode: 'advanced' as 'advanced',
            builderConfig: {functions: []},
            aggregateWindow: {period: '2000', fillValues: false},
          },
        ],
      },
    }

    const returnedView = applyAutoAggregateRequirements(advancedModeQuery)

    expect(returnedView).toEqual(advancedModeQuery)
  })

  it('does not alter builder queries which can be represented in the autoAggregating Builder', () => {
    const representableConfig = cloneDeep(autoAggregateableBuilderConfig)

    const returnedConfig = applyAutoAggregateRequirements(representableConfig)

    expect(returnedConfig).toEqual(representableConfig)
  })

  describe('converts a "builder" query with a builderConfig that can not be represented in the new autoAggregating builder to an "advanced" query, displaying in the script editor instead, and modifies the builderConfig so that user gets warning when switching to queryBuilder', () => {
    it('if the builderConfig can not be represented in autoAggregating builder because it does not have any aggregationFunctions', () => {
      const initBuilderConfig = {functions: []}
      const originalQueryText = 'something'
      const builderViewWithNoFunctions = {
        properties: {
          queries: [
            {
              text: originalQueryText,
              editMode: 'builder' as 'builder',
              builderConfig: initBuilderConfig,
              aggregateWindow: {period: '2000', fillValues: false},
            },
          ],
        },
      }

      const returnedView = applyAutoAggregateRequirements(
        builderViewWithNoFunctions
      )

      const {editMode, builderConfig, text} = returnedView.properties.queries[0]

      expect(editMode).toBe('advanced')
      expect(text).toEqual(originalQueryText)
      expect(builderConfig).not.toEqual(initBuilderConfig)
      expect(hasQueryBeenEdited(text, builderConfig)).toBeTruthy()
    })
  })

  it('returns an "advanced" config with query text unaltered if changes are needed but a query cant be built from new builder config', () => {
    const originalQueryText = 'something'
    const builderConfigThatCantBeBuilt = {
      properties: {
        queries: [
          {
            text: originalQueryText,
            editMode: 'builder' as 'builder',
            builderConfig: {
              functions: [{name: 'moo'}],
              aggregateWindow: {period: '2000'},
            },
          },
        ],
      },
    }

    const returnedView = applyAutoAggregateRequirements(
      builderConfigThatCantBeBuilt
    )

    const {text, editMode} = returnedView.properties.queries[0]

    expect(text).toEqual(originalQueryText)
    expect(editMode).toBe('advanced')
  })

  it('adds period and fillValues if aggregateWindow does not contain them', () => {
    const initBuilderConfig = cloneDeep(autoAggregateableBuilderConfig)

    initBuilderConfig.properties.queries[0].builderConfig.aggregateWindow = {}

    const returnedView = applyAutoAggregateRequirements(initBuilderConfig)

    const {
      editMode,
      builderConfig,
      builderConfig: {
        aggregateWindow: {period, fillValues},
      },
    } = returnedView.properties.queries[0]

    expect(editMode).toBe('builder')
    expect(builderConfig).not.toEqual(initBuilderConfig)
    expect(period).toBe(AGG_WINDOW_AUTO)
    expect(fillValues).toBe(DEFAULT_FILLVALUES)
  })

  it('adds a period if aggregateWindow does not contain it', () => {
    const initBuilderConfig = cloneDeep(autoAggregateableBuilderConfig)
    const initFillValues = true
    initBuilderConfig.properties.queries[0].builderConfig.aggregateWindow = {
      fillValues: initFillValues,
    }

    const returnedView = applyAutoAggregateRequirements(initBuilderConfig)

    const {
      editMode,
      builderConfig,
      builderConfig: {
        aggregateWindow: {period, fillValues},
      },
    } = returnedView.properties.queries[0]

    expect(editMode).toBe('builder')
    expect(builderConfig).not.toEqual(initBuilderConfig)
    expect(period).toBe(AGG_WINDOW_AUTO)
    expect(fillValues).toBe(initFillValues)
  })

  it('adds fillValues if aggregateWindow does not contain it', () => {
    const initBuilderConfig = cloneDeep(autoAggregateableBuilderConfig)
    const initPeriod = 'auto'
    initBuilderConfig.properties.queries[0].builderConfig.aggregateWindow = {
      period: initPeriod,
    }

    const returnedView = applyAutoAggregateRequirements(initBuilderConfig)

    const {
      editMode,
      builderConfig,
      builderConfig: {
        aggregateWindow: {period, fillValues},
      },
    } = returnedView.properties.queries[0]

    expect(editMode).toBe('builder')
    expect(builderConfig).not.toEqual(initBuilderConfig)
    expect(period).toBe(initPeriod)
    expect(fillValues).toBe(DEFAULT_FILLVALUES)
  })
})
