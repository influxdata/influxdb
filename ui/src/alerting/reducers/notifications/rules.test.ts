import rulesReducer, {
  defaultNotificationRulesState,
} from 'src/alerting/reducers/notifications/rules'
import {
  setAllNotificationRules,
  setRule,
  setCurrentRule,
  removeRule,
} from 'src/alerting/actions/notifications/rules'
import {RemoteDataState} from 'src/types'
import {rule} from 'src/alerting/constants'

describe('rulesReducer', () => {
  describe('setAllNotificationRules', () => {
    it('sets list and status properties of state.', () => {
      const initialState = defaultNotificationRulesState

      const actual = rulesReducer(
        initialState,
        setAllNotificationRules(RemoteDataState.Done, [rule])
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [rule],
        status: RemoteDataState.Done,
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('setRule', () => {
    it('adds rule to list if it is new', () => {
      const initialState = defaultNotificationRulesState

      const actual = rulesReducer(initialState, setRule(rule))

      const expected = {
        ...defaultNotificationRulesState,
        list: [rule],
      }

      expect(actual).toEqual(expected)
    })

    it('updates rule in list if it exists', () => {
      let initialState = defaultNotificationRulesState
      initialState.list = [rule]

      const actual = rulesReducer(
        initialState,
        setRule({
          ...rule,
          name: 'moo',
        })
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [{...rule, name: 'moo'}],
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('removeRule', () => {
    it('removes rule from list', () => {
      const initialState = defaultNotificationRulesState
      initialState.list = [rule]
      const actual = rulesReducer(initialState, removeRule(rule.id))

      const expected = {
        ...defaultNotificationRulesState,
        list: [],
      }

      expect(actual).toEqual(expected)
    })
  })

  describe('setCurrentRule', () => {
    it('sets current rule and status.', () => {
      const initialState = defaultNotificationRulesState

      const actual = rulesReducer(
        initialState,
        setCurrentRule(RemoteDataState.Done, rule)
      )

      const expected = {
        ...defaultNotificationRulesState,
        current: {
          status: RemoteDataState.Done,
          rule: rule,
        },
      }

      expect(actual).toEqual(expected)
    })
  })
})
