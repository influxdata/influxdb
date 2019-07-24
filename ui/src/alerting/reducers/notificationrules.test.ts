import notificationRulesReducer, {
  defaultNotificationRulesState,
} from 'src/alerting/reducers/notificationRules'
import {
  setAllNotificationRules,
  setNotificationRule,
  setCurrentNotificationRule,
  removeNotificationRule,
} from 'src/alerting/actions/notificationRules'
import {RemoteDataState} from 'src/types'
import {notificationRule} from 'src/alerting/constants'

describe('notificationRulesReducer', () => {
  describe('setAllNotificationRules', () => {
    it('sets list and status properties of state.', () => {
      const initialState = defaultNotificationRulesState

      const actual = notificationRulesReducer(
        initialState,
        setAllNotificationRules(RemoteDataState.Done, [notificationRule])
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [notificationRule],
        status: RemoteDataState.Done,
      }

      expect(actual).toEqual(expected)
    })
  })
  describe('setNotificationRule', () => {
    it('adds notificationRule to list if it is new', () => {
      const initialState = defaultNotificationRulesState

      const actual = notificationRulesReducer(
        initialState,
        setNotificationRule(notificationRule)
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [notificationRule],
      }

      expect(actual).toEqual(expected)
    })
    it('updates notificationRule in list if it exists', () => {
      let initialState = defaultNotificationRulesState
      initialState.list = [notificationRule]

      const actual = notificationRulesReducer(
        initialState,
        setNotificationRule({
          ...notificationRule,
          name: 'moo',
        })
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [{...notificationRule, name: 'moo'}],
      }

      expect(actual).toEqual(expected)
    })
  })
  describe('removeNotificationRule', () => {
    it('removes notificationRule from list', () => {
      const initialState = defaultNotificationRulesState
      initialState.list = [notificationRule]
      const actual = notificationRulesReducer(
        initialState,
        removeNotificationRule(notificationRule.id)
      )

      const expected = {
        ...defaultNotificationRulesState,
        list: [],
      }

      expect(actual).toEqual(expected)
    })
  })
  describe('setCurrentNotificationRule', () => {
    it('sets current notificationRule and status.', () => {
      const initialState = defaultNotificationRulesState

      const actual = notificationRulesReducer(
        initialState,
        setCurrentNotificationRule(RemoteDataState.Done, notificationRule)
      )

      const expected = {
        ...defaultNotificationRulesState,
        current: {
          status: RemoteDataState.Done,
          notificationRule: notificationRule,
        },
      }

      expect(actual).toEqual(expected)
    })
  })
})
