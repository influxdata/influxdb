import reducer from 'src/data_explorer/reducers/queryConfigs'

import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  fill,
  timeShift,
  chooseTag,
  groupByTag,
  groupByTime,
  toggleField,
  removeFuncs,
  updateRawQuery,
  editQueryStatus,
  chooseNamespace,
  chooseMeasurement,
  applyFuncsToField,
  addInitialField,
  updateQueryConfig,
  toggleTagAcceptance,
} from 'src/data_explorer/actions/view'

import {LINEAR, NULL_STRING} from 'shared/constants/queryFillOptions'

const fakeAddQueryAction = (panelID, queryID) => {
  return {
    type: 'DE_ADD_QUERY',
    payload: {panelID, queryID},
  }
}

function buildInitialState(queryID, params) {
  return Object.assign({}, defaultQueryConfig({id: queryID}), params)
}

describe('Chronograf.Reducers.DataExplorer.queryConfigs', () => {
  const queryID = 123

  it('can add a query', () => {
    const state = reducer({}, fakeAddQueryAction('blah', queryID))

    const actual = state[queryID]
    const expected = defaultQueryConfig({id: queryID})
    expect(actual).to.deep.equal(expected)
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

      expect(newState[queryID].database).to.equal('telegraf')
      expect(newState[queryID].retentionPolicy).to.equal('monitor')
    })

    it('sets the measurement', () => {
      const newState = reducer(state, chooseMeasurement(queryID, 'mem'))

      expect(newState[queryID].measurement).to.equal('mem')
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
        addInitialField(queryID, {
          value: 'a great field',
          type: 'field',
        })
      )
    })

    describe('choosing a new namespace', () => {
      it('clears out the old measurement and fields', () => {
        // what about tags?
        expect(state[queryID].measurement).to.equal('disk')
        expect(state[queryID].fields.length).to.equal(1)

        const newState = reducer(
          state,
          chooseNamespace(queryID, {
            database: 'newdb',
            retentionPolicy: 'newrp',
          })
        )

        expect(newState[queryID].measurement).to.be.null
        expect(newState[queryID].fields.length).to.equal(0)
      })
    })

    describe('choosing a new measurement', () => {
      it('leaves the namespace and clears out the old fields', () => {
        // what about tags?
        expect(state[queryID].fields.length).to.equal(1)

        const newState = reducer(
          state,
          chooseMeasurement(queryID, 'newmeasurement')
        )

        expect(state[queryID].database).to.equal(newState[queryID].database)
        expect(state[queryID].retentionPolicy).to.equal(
          newState[queryID].retentionPolicy
        )
        expect(newState[queryID].fields.length).to.equal(0)
      })
    })

    describe('DE_TOGGLE_FIELD', () => {
      it('can toggle multiple fields', () => {
        expect(state[queryID].fields.length).to.equal(1)

        const newState = reducer(
          state,
          toggleField(queryID, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryID].fields.length).to.equal(2)
        expect(newState[queryID].fields[1].alias).to.deep.equal('mean_f2')
        expect(newState[queryID].fields[1].args).to.deep.equal([
          {value: 'f2', type: 'field'},
        ])
        expect(newState[queryID].fields[1].value).to.deep.equal('mean')
      })

      it('applies a func to newly selected fields', () => {
        expect(state[queryID].fields.length).to.equal(1)
        expect(state[queryID].fields[0].type).to.equal('func')
        expect(state[queryID].fields[0].value).to.equal('mean')

        const newState = reducer(
          state,
          toggleField(queryID, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryID].fields[1].value).to.equal('mean')
        expect(newState[queryID].fields[1].alias).to.equal('mean_f2')
        expect(newState[queryID].fields[1].args).to.deep.equal([
          {value: 'f2', type: 'field'},
        ])
        expect(newState[queryID].fields[1].type).to.equal('func')
      })

      it('adds the field property to query config if not found', () => {
        delete state[queryID].fields
        expect(state[queryID].fields).to.equal(undefined)

        const newState = reducer(
          state,
          toggleField(queryID, {value: 'fk1', type: 'field'})
        )

        expect(newState[queryID].fields.length).to.equal(1)
      })
    })
  })

  describe('DE_APPLY_FUNCS_TO_FIELD', () => {
    it('applies new functions to a field', () => {
      const f1 = {value: 'f1', type: 'field'}
      const f2 = {value: 'f2', type: 'field'}
      const f3 = {value: 'f3', type: 'field'}
      const f4 = {value: 'f4', type: 'field'}

      const initialState = {
        [queryID]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [
            {value: 'fn1', type: 'func', args: [f1], alias: `fn1_${f1.value}`},
            {value: 'fn1', type: 'func', args: [f2], alias: `fn1_${f2.value}`},
            {value: 'fn2', type: 'func', args: [f1], alias: `fn2_${f1.value}`},
          ],
        },
      }

      const action = applyFuncsToField(queryID, {
        field: {value: 'f1', type: 'field'},
        funcs: [
          {value: 'fn3', type: 'func', args: []},
          {value: 'fn4', type: 'func', args: []},
        ],
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fields).to.deep.equal([
        {value: 'fn3', type: 'func', args: [f1], alias: `fn3_${f1.value}`},
        {value: 'fn4', type: 'func', args: [f1], alias: `fn4_${f1.value}`},
        {value: 'fn1', type: 'func', args: [f2], alias: `fn1_${f2.value}`},
      ])
    })
  })

  describe('DE_REMOVE_FUNCS', () => {
    it('removes all functions and group by time when one field has no funcs applied', () => {
      const f1 = {value: 'f1', type: 'field'}
      const f2 = {value: 'f2', type: 'field'}
      const fields = [
        {value: 'fn1', type: 'func', args: [f1], alias: `fn1_${f1.value}`},
        {value: 'fn1', type: 'func', args: [f2], alias: `fn1_${f2.value}`},
      ]
      const groupBy = {time: '1m', tags: []}

      const initialState = {
        [queryID]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields,
          groupBy,
        },
      }

      const action = removeFuncs(queryID, fields, groupBy)

      const nextState = reducer(initialState, action)
      const actual = nextState[queryID].fields
      const expected = [f1, f2]

      expect(actual).to.eql(expected)
      expect(nextState[queryID].groupBy.time).to.equal(null)
    })
  })

  describe('DE_CHOOSE_TAG', () => {
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

      expect(nextState[queryID].tags).to.eql({
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

      expect(nextState[queryID].tags).to.eql({
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
      expect(nextState[queryID].tags).to.eql({})
    })
  })

  describe('DE_GROUP_BY_TAG', () => {
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

      expect(nextState[queryID].groupBy).to.eql({
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

      expect(nextState[queryID].groupBy).to.eql({
        time: null,
        tags: [],
      })
    })
  })

  describe('DE_TOGGLE_TAG_ACCEPTANCE', () => {
    it('it toggles areTagsAccepted', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const action = toggleTagAcceptance(queryID)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].areTagsAccepted).to.equal(
        !initialState[queryID].areTagsAccepted
      )
    })
  })

  describe('DE_GROUP_BY_TIME', () => {
    it('applys the appropriate group by time', () => {
      const time = '100y'
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const action = groupByTime(queryID, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy.time).to.equal(time)
    })
  })

  it('updates entire config', () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const expected = defaultQueryConfig({id: queryID}, {rawText: 'hello'})
    const action = updateQueryConfig(expected)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID]).to.deep.equal(expected)
  })

  it("updates a query's raw text", () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const text = 'foo'
    const action = updateRawQuery(queryID, text)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID].rawText).to.equal('foo')
  })

  it("updates a query's raw status", () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const status = 'your query was sweet'
    const action = editQueryStatus(queryID, status)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID].status).to.equal(status)
  })

  describe('DE_FILL', () => {
    it('applies an explicit fill when group by time is used', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const time = '10s'
      const action = groupByTime(queryID, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).to.equal(NULL_STRING)
    })

    it('updates fill to non-null-string non-number string value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const action = fill(queryID, LINEAR)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).to.equal(LINEAR)
    })

    it('updates fill to string integer value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const INT_STRING = '1337'
      const action = fill(queryID, INT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).to.equal(INT_STRING)
    })

    it('updates fill to string float value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const FLOAT_STRING = '1.337'
      const action = fill(queryID, FLOAT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).to.equal(FLOAT_STRING)
    })
  })

  describe('DE_TIME_SHIFT', () => {
    it('can shift the time', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const shift = {quantity: 1, unit: 'd', duration: '1d'}
      const action = timeShift(queryID, shift)
      const nextState = reducer(initialState, action)

      expect(nextState[queryID].shifts).to.deep.equal([shift])
    })
  })
})
