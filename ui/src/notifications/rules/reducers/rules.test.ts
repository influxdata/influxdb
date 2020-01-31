// Libraries
import {normalize} from 'normalizr'

// Schemas
import {arrayOfRules, rule as ruleSchema} from 'src/schemas'

// Reducers
import rulesReducer, {
  defaultNotificationRulesState,
} from 'src/notifications/rules/reducers'

import {
  setRules,
  setRule,
  setCurrentRule,
  removeRule,
} from 'src/notifications/rules/actions/creators'

import {initRuleDraft} from 'src/notifications/rules/utils'

import {RemoteDataState, RuleEntities, NotificationRule} from 'src/types'

const ruleID = '1'
const NEW_RULE_DRAFT = {
  ...initRuleDraft(''),
  id: ruleID,
  statusRules: [],
}

describe('rulesReducer', () => {
  describe('setAllNotificationRules', () => {
    it('sets list and status properties of state.', () => {
      const initialState = defaultNotificationRulesState

      const rules = normalize<NotificationRule, RuleEntities, string[]>(
        [NEW_RULE_DRAFT],
        arrayOfRules
      )

      const actual = rulesReducer(
        initialState,
        setRules(RemoteDataState.Done, rules)
      )

      const expected = {
        ...NEW_RULE_DRAFT,
        loadingStatus: RemoteDataState.Done,
      }

      expect(actual.status).toEqual(RemoteDataState.Done)
      expect(actual.byID[ruleID]).toEqual(expected)
      expect(actual.allIDs).toEqual([ruleID])
    })
  })

  describe('setRule', () => {
    it('adds rule to list if it is new', () => {
      const initialState = defaultNotificationRulesState

      const rule = normalize<NotificationRule, RuleEntities, string>(
        NEW_RULE_DRAFT,
        ruleSchema
      )

      const actual = rulesReducer(
        initialState,
        setRule(ruleID, RemoteDataState.Done, rule)
      )

      const expected = {
        ...NEW_RULE_DRAFT,
        loadingStatus: RemoteDataState.Done,
      }

      expect(actual.byID[ruleID]).toEqual(expected)
      expect(actual.allIDs).toEqual([ruleID])
    })

    it('updates rule in list if it exists', () => {
      const initialState = defaultNotificationRulesState
      const rule = {...NEW_RULE_DRAFT, name: 'moo'}

      const normRule = normalize<NotificationRule, RuleEntities, string>(
        rule,
        ruleSchema
      )

      const actual = rulesReducer(
        initialState,
        setRule(ruleID, RemoteDataState.Done, normRule)
      )

      const expected = {
        ...rule,
        loadingStatus: RemoteDataState.Done,
      }

      expect(actual.byID[ruleID]).toEqual(expected)
    })
  })

  describe('removeRule', () => {
    it('removes rule from list', () => {
      const initialState = defaultNotificationRulesState
      const actual = rulesReducer(initialState, removeRule(NEW_RULE_DRAFT.id))

      expect(actual.allIDs).toEqual([])
      expect(actual.byID).toEqual({})
    })
  })

  describe('setCurrentRule', () => {
    it('sets current rule and status.', () => {
      const initialState = defaultNotificationRulesState

      const rule = normalize<NotificationRule, RuleEntities, string>(
        NEW_RULE_DRAFT,
        ruleSchema
      )

      const actual = rulesReducer(
        initialState,
        setCurrentRule(RemoteDataState.Done, rule)
      )

      expect(actual.current.status).toEqual(RemoteDataState.Done)
      expect(actual.current.rule).toEqual({
        ...NEW_RULE_DRAFT,
        loadingStatus: RemoteDataState.Done,
      })
    })
  })
})
