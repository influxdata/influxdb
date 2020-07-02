import applyQueryBuilderRequirements from 'src/utils/defaultAggregate'
import {hasQueryBeenEdited} from 'src/timeMachine/utils/queryBuilder'

describe('defaultAggregate', () => {
  it('converts a builder view with no functions to an advanced view and modifies builder config (so that switching to builder mode in time machine alerts user)', () => {
    const initBuilderConfig = {functions: []}
    const builderViewWithNoFunctions = {
      properties: {
        queries: [
          {
            text: 'something',
            editMode: 'builder' as 'builder',
            builderConfig: initBuilderConfig,
          },
        ],
      },
    }

    const returnedView = applyQueryBuilderRequirements(
      builderViewWithNoFunctions
    )

    const {editMode, builderConfig, text} = returnedView.properties.queries[0]

    expect(editMode).toBe('advanced')
    expect(builderConfig).not.toEqual(initBuilderConfig)
    expect(hasQueryBeenEdited(text, builderConfig)).toBeTruthy()
  })
  it('does not change advanced queries', () => {
    const builderViewWithNoFunctions = {
      properties: {
        queries: [
          {
            text: 'something',
            editMode: 'advanced' as 'advanced',
            builderConfig: {functions: []},
          },
        ],
      },
    }

    const returnedView = applyQueryBuilderRequirements(
      builderViewWithNoFunctions
    )

    expect(returnedView).toEqual(builderViewWithNoFunctions)
  })
  it('does not change builder queries which contain an aggregate function', () => {
    const builderViewWithNoFunctions = {
      properties: {
        queries: [
          {
            text: 'something',
            editMode: 'builder' as 'builder',
            builderConfig: {functions: [{name: 'moo'}]},
          },
        ],
      },
    }

    const returnedView = applyQueryBuilderRequirements(
      builderViewWithNoFunctions
    )

    expect(returnedView).toEqual(builderViewWithNoFunctions)
  })
})
