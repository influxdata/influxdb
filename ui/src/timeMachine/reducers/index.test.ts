import {createStore} from 'redux'

import {
  initialState,
  initialStateHelper,
  timeMachinesReducer,
} from 'src/timeMachine/reducers'

describe('the Time Machine reducer', () => {
  describe('setting the default aggregateFunctionType', () => {
    const store = createStore(timeMachinesReducer, initialState())
    const expectedAggregatefunctionType = initialStateHelper().queryBuilder
      .tags[0].aggregateFunctionType

    it('is set when setting a builder bucket selection', () => {
      store.dispatch({
        type: 'SET_BUILDER_BUCKET_SELECTION',
        payload: {bucket: 'foo', resetSelections: true},
      })
      const actualState = store.getState()
      const defaultAggregateFunctionType =
        actualState.timeMachines.de.draftQueries[0].builderConfig.tags[0]
          .aggregateFunctionType

      expect(defaultAggregateFunctionType).toEqual(
        expectedAggregatefunctionType
      )
    })

    it('is set when adding a new tag selector', () => {
      store.dispatch({type: 'ADD_TAG_SELECTOR'})
      const actualState = store.getState()
      const defaultAggregateFunctionType =
        actualState.timeMachines.de.draftQueries[0].builderConfig.tags[0]
          .aggregateFunctionType

      expect(defaultAggregateFunctionType).toEqual(
        expectedAggregatefunctionType
      )
    })
  })
})
