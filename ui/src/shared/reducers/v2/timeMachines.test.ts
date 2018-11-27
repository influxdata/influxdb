import timeMachinesReducer, {
  initialState,
} from 'src/shared/reducers/v2/timeMachines'
import {submitScript, setQuerySource} from 'src/shared/actions/v2/timeMachines'
import {DE_TIME_MACHINE_ID} from 'src/shared/constants/timeMachine'
import {DashboardQuery, InfluxLanguage} from 'src/types/v2/dashboards'

describe('timeMachinesReducer', () => {
  describe('SUBMIT_SCRIPT', () => {
    test('replaces the first queries text if it exists', () => {
      const state = initialState()
      const timeMachine = state.timeMachines[DE_TIME_MACHINE_ID]
      const viewProps = timeMachine.view.properties
      const initialQuery: DashboardQuery = {
        text: 'foo',
        type: InfluxLanguage.Flux,
        sourceID: 'bar',
      }

      viewProps.queries = [initialQuery]
      timeMachine.draftScript = 'baz'

      const nextState = timeMachinesReducer(state, submitScript())
      const nextViewProps =
        nextState.timeMachines[DE_TIME_MACHINE_ID].view.properties

      expect(nextViewProps.queries[0]).toEqual({...initialQuery, text: 'baz'})
    })

    test('inserts a query if no queries exist', () => {
      const state = initialState()
      const timeMachine = state.timeMachines[DE_TIME_MACHINE_ID]
      const viewProps = timeMachine.view.properties

      viewProps.queries = []
      timeMachine.draftScript = 'baz'

      const nextState = timeMachinesReducer(state, submitScript())
      const nextViewProps =
        nextState.timeMachines[DE_TIME_MACHINE_ID].view.properties

      expect(nextViewProps.queries[0]).toEqual({
        text: 'baz',
        type: InfluxLanguage.Flux,
        sourceID: '',
      })
    })
  })

  describe('SET_QUERY_SOURCE', () => {
    test('replaces the sourceID for the active query', () => {
      const state = initialState()
      const timeMachine = state.timeMachines[DE_TIME_MACHINE_ID]
      const viewProps = timeMachine.view.properties

      expect(viewProps.queries[0].sourceID).toEqual('')

      const nextState = timeMachinesReducer(state, setQuerySource('howdy'))
      const nextViewProps =
        nextState.timeMachines[DE_TIME_MACHINE_ID].view.properties

      expect(nextViewProps.queries[0].sourceID).toEqual('howdy')
    })

    test('does nothing if the no active query exists', () => {
      const state = initialState()
      const timeMachine = state.timeMachines[DE_TIME_MACHINE_ID]
      const viewProps = timeMachine.view.properties

      viewProps.queries = []

      const nextState = timeMachinesReducer(state, setQuerySource('howdy'))
      const nextViewProps =
        nextState.timeMachines[DE_TIME_MACHINE_ID].view.properties

      expect(nextViewProps.queries).toEqual([])
    })
  })
})
