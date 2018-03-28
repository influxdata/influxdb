import reducer from 'src/kapacitor/reducers/queryConfigs'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  chooseTag,
  timeShift,
  groupByTag,
  toggleField,
  groupByTime,
  chooseNamespace,
  chooseMeasurement,
  applyFuncsToField,
  toggleTagAcceptance,
} from 'src/kapacitor/actions/queryConfigs'

const fakeAddQueryAction = (panelID, queryID) => {
  return {
    type: 'KAPA_ADD_QUERY',
    payload: {panelID, queryID},
  }
}

function buildInitialState(queryID, params) {
  return Object.assign(
    {},
    defaultQueryConfig({id: queryID, isKapacitorRule: true}),
    params
  )
}

describe('Chronograf.Reducers.Kapacitor.queryConfigs', () => {
  const queryID = 123

  it('can add a query', () => {
    const state = reducer({}, fakeAddQueryAction('blah', queryID))

    const actual = state[queryID]
    const expected = defaultQueryConfig({id: queryID, isKapacitorRule: true})
    expect(actual).toEqual(expected)
  })

  describe('choosing db, rp, and measurement', () => {
    let state
    beforeEach(() => {
      state = reducer({}, fakeAddQueryAction('any', queryID))
    })

    it('sets the db and rp', () => {
      const newState = reducer(
        state,
        chooseNamespace(queryID, {
          database: 'telegraf',
          retentionPolicy: 'monitor',
        })
      )

      expect(newState[queryID].database).toBe('telegraf')
      expect(newState[queryID].retentionPolicy).toBe('monitor')
    })

    it('sets the measurement', () => {
      const newState = reducer(state, chooseMeasurement(queryID, 'mem'))

      expect(newState[queryID].measurement).toBe('mem')
    })
  })

  describe('a query has measurements and fields', () => {
    let state
    beforeEach(() => {
      const one = reducer({}, fakeAddQueryAction('any', queryID))
      const two = reducer(
        one,
        chooseNamespace(queryID, {
          database: '_internal',
          retentionPolicy: 'daily',
        })
      )
      const three = reducer(two, chooseMeasurement(queryID, 'disk'))
      state = reducer(
        three,
        toggleField(queryID, {value: 'a great field', funcs: []})
      )
    })

    describe('choosing a new namespace', () => {
      it('clears out the old measurement and fields', () => {
        // what about tags?
        expect(state[queryID].measurement).toBeTruthy()
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          chooseNamespace(queryID, {
            database: 'newdb',
            retentionPolicy: 'newrp',
          })
        )

        expect(newState[queryID].measurement).toBeFalsy()
        expect(newState[queryID].fields.length).toBe(0)
      })
    })

    describe('choosing a new measurement', () => {
      it('leaves the namespace and clears out the old fields', () => {
        // what about tags?
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          chooseMeasurement(queryID, 'newmeasurement')
        )

        expect(state[queryID].database).toBe(newState[queryID].database)
        expect(state[queryID].retentionPolicy).toBe(
          newState[queryID].retentionPolicy
        )
        expect(newState[queryID].fields.length).toBe(0)
      })
    })

    describe('when the query is part of a kapacitor rule', () => {
      it('only allows one field', () => {
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          toggleField(queryID, {value: 'a different field', type: 'field'})
        )

        expect(newState[queryID].fields.length).toBe(1)
        expect(newState[queryID].fields[0].value).toBe('a different field')
      })
    })

    describe('KAPA_TOGGLE_FIELD', () => {
      it('cannot toggle multiple fields', () => {
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          toggleField(queryID, {value: 'a different field', type: 'field'})
        )

        expect(newState[queryID].fields.length).toBe(1)
        expect(newState[queryID].fields[0].value).toBe('a different field')
      })

      it('applies no funcs to newly selected fields', () => {
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          toggleField(queryID, {value: 'a different field', type: 'field'})
        )

        expect(newState[queryID].fields[0].type).toBe('field')
      })
    })
  })

  describe('KAPA_APPLY_FUNCS_TO_FIELD', () => {
    it('applies functions to a field without any existing functions', () => {
      const f1 = {value: 'f1', type: 'field'}
      const initialState = {
        [queryID]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [f1],
          groupBy: {
            tags: [],
            time: null,
          },
        },
      }

      const action = applyFuncsToField(queryID, {
        field: {value: 'f1', type: 'field'},
        funcs: [{value: 'fn3', type: 'func'}, {value: 'fn4', type: 'func'}],
      })

      const nextState = reducer(initialState, action)
      const actual = nextState[queryID].fields
      const expected = [
        {value: 'fn3', type: 'func', args: [f1], alias: `fn3_${f1.value}`},
        {value: 'fn4', type: 'func', args: [f1], alias: `fn4_${f1.value}`},
      ]

      expect(actual).toEqual(expected)
    })
  })

  describe('KAPA_CHOOSE_TAG', () => {
    it('adds a tag key/value to the query', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID, {
          tags: {
            k1: ['v0'],
            k2: ['foo'],
          },
        }),
      }
      const action = chooseTag(queryID, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].tags).toEqual({
        k1: ['v0', 'v1'],
        k2: ['foo'],
      })
    })

    it("creates a new entry if it's the first key", () => {
      const initialState = {
        [queryID]: buildInitialState(queryID, {
          tags: {},
        }),
      }
      const action = chooseTag(queryID, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].tags).toEqual({
        k1: ['v1'],
      })
    })

    it('removes a value that is already in the list', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID, {
          tags: {
            k1: ['v1'],
          },
        }),
      }
      const action = chooseTag(queryID, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      // TODO: this should probably remove the `k1` property entirely from the tags object
      expect(nextState[queryID].tags).toEqual({})
    })
  })

  describe('KAPA_GROUP_BY_TAG', () => {
    it('adds a tag key/value to the query', () => {
      const initialState = {
        [queryID]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [],
          tags: {},
          groupBy: {tags: [], time: null},
        },
      }
      const action = groupByTag(queryID, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy).toEqual({
        time: null,
        tags: ['k1'],
      })
    })

    it('removes a tag if the given tag key is already in the GROUP BY list', () => {
      const initialState = {
        [queryID]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [],
          tags: {},
          groupBy: {tags: ['k1'], time: null},
        },
      }
      const action = groupByTag(queryID, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy).toEqual({
        time: null,
        tags: [],
      })
    })
  })

  describe('KAPA_TOGGLE_TAG_ACCEPTANCE', () => {
    it('it toggles areTagsAccepted', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const action = toggleTagAcceptance(queryID)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].areTagsAccepted).toBe(
        !initialState[queryID].areTagsAccepted
      )
    })
  })

  describe('KAPA_GROUP_BY_TIME', () => {
    it('applys the appropriate group by time', () => {
      const time = '100y'
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const action = groupByTime(queryID, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy.time).toBe(time)
    })
  })

  describe('KAPA_TIME_SHIFT', () => {
    it('can shift the time', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const shift = {quantity: 1, unit: 'd', duration: '1d'}
      const action = timeShift(queryID, shift)
      const nextState = reducer(initialState, action)

      expect(nextState[queryID].shifts).toEqual([shift])
    })
  })
})
