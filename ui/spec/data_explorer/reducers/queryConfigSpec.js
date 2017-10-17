import reducer from 'src/data_explorer/reducers/queryConfigs'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  fill,
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

function buildInitialState(queryId, params) {
  return Object.assign({}, defaultQueryConfig({id: queryId}), params)
}

describe('Chronograf.Reducers.DataExplorer.queryConfigs', () => {
  const queryId = 123

  it('can add a query', () => {
    const state = reducer({}, fakeAddQueryAction('blah', queryId))

    const actual = state[queryId]
    const expected = defaultQueryConfig({id: queryId})
    expect(actual).to.deep.equal(expected)
  })

  describe('choosing db, rp, and measurement', () => {
    let state
    beforeEach(() => {
      state = reducer({}, fakeAddQueryAction('any', queryId))
    })

    it('sets the db and rp', () => {
      const newState = reducer(
        state,
        chooseNamespace(queryId, {
          database: 'telegraf',
          retentionPolicy: 'monitor',
        })
      )

      expect(newState[queryId].database).to.equal('telegraf')
      expect(newState[queryId].retentionPolicy).to.equal('monitor')
    })

    it('sets the measurement', () => {
      const newState = reducer(state, chooseMeasurement(queryId, 'mem'))

      expect(newState[queryId].measurement).to.equal('mem')
    })
  })

  describe('a query has measurements and fields', () => {
    let state
    beforeEach(() => {
      const one = reducer({}, fakeAddQueryAction('any', queryId))
      const two = reducer(
        one,
        chooseNamespace(queryId, {
          database: '_internal',
          retentionPolicy: 'daily',
        })
      )
      const three = reducer(two, chooseMeasurement(queryId, 'disk'))

      state = reducer(
        three,
        addInitialField(queryId, {
          value: 'a great field',
          type: 'field',
        })
      )
    })

    describe('choosing a new namespace', () => {
      it('clears out the old measurement and fields', () => {
        // what about tags?
        expect(state[queryId].measurement).to.equal('disk')
        expect(state[queryId].fields.length).to.equal(1)

        const newState = reducer(
          state,
          chooseNamespace(queryId, {
            database: 'newdb',
            retentionPolicy: 'newrp',
          })
        )

        expect(newState[queryId].measurement).to.be.null
        expect(newState[queryId].fields.length).to.equal(0)
      })
    })

    describe('choosing a new measurement', () => {
      it('leaves the namespace and clears out the old fields', () => {
        // what about tags?
        expect(state[queryId].fields.length).to.equal(1)

        const newState = reducer(
          state,
          chooseMeasurement(queryId, 'newmeasurement')
        )

        expect(state[queryId].database).to.equal(newState[queryId].database)
        expect(state[queryId].retentionPolicy).to.equal(
          newState[queryId].retentionPolicy
        )
        expect(newState[queryId].fields.length).to.equal(0)
      })
    })

    describe('DE_TOGGLE_FIELD', () => {
      it('can toggle multiple fields', () => {
        expect(state[queryId].fields.length).to.equal(1)

        const newState = reducer(
          state,
          toggleField(queryId, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryId].fields.length).to.equal(2)
        expect(newState[queryId].fields[1].alias).to.deep.equal('mean_f2')
        expect(newState[queryId].fields[1].args).to.deep.equal([
          {value: 'f2', type: 'field'},
        ])
        expect(newState[queryId].fields[1].value).to.deep.equal('mean')
      })

      it('applies a func to newly selected fields', () => {
        expect(state[queryId].fields.length).to.equal(1)
        expect(state[queryId].fields[0].type).to.equal('func')
        expect(state[queryId].fields[0].value).to.equal('mean')

        const newState = reducer(
          state,
          toggleField(queryId, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryId].fields[1].value).to.equal('mean')
        expect(newState[queryId].fields[1].alias).to.equal('mean_f2')
        expect(newState[queryId].fields[1].args).to.deep.equal([
          {value: 'f2', type: 'field'},
        ])
        expect(newState[queryId].fields[1].type).to.equal('func')
      })

      it('adds the field property to query config if not found', () => {
        delete state[queryId].fields
        expect(state[queryId].fields).to.equal(undefined)

        const newState = reducer(
          state,
          toggleField(queryId, {value: 'fk1', type: 'field'})
        )

        expect(newState[queryId].fields.length).to.equal(1)
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
        [queryId]: {
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

      const action = applyFuncsToField(queryId, {
        field: {value: 'f1', type: 'field'},
        funcs: [
          {value: 'fn3', type: 'func', args: []},
          {value: 'fn4', type: 'func', args: []},
        ],
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fields).to.deep.equal([
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
        [queryId]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields,
          groupBy,
        },
      }

      const action = removeFuncs(queryId, fields, groupBy)

      const nextState = reducer(initialState, action)
      const actual = nextState[queryId].fields
      const expected = [f1, f2]

      expect(actual).to.eql(expected)
      expect(nextState[queryId].groupBy.time).to.equal(null)
    })
  })

  describe('DE_CHOOSE_TAG', () => {
    it('adds a tag key/value to the query', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId, {
          tags: {
            k1: ['v0'],
            k2: ['foo'],
          },
        }),
      }
      const action = chooseTag(queryId, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].tags).to.eql({
        k1: ['v0', 'v1'],
        k2: ['foo'],
      })
    })

    it("creates a new entry if it's the first key", () => {
      const initialState = {
        [queryId]: buildInitialState(queryId, {
          tags: {},
        }),
      }
      const action = chooseTag(queryId, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].tags).to.eql({
        k1: ['v1'],
      })
    })

    it('removes a value that is already in the list', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId, {
          tags: {
            k1: ['v1'],
          },
        }),
      }
      const action = chooseTag(queryId, {
        key: 'k1',
        value: 'v1',
      })

      const nextState = reducer(initialState, action)

      // TODO: this should probably remove the `k1` property entirely from the tags object
      expect(nextState[queryId].tags).to.eql({})
    })
  })

  describe('DE_GROUP_BY_TAG', () => {
    it('adds a tag key/value to the query', () => {
      const initialState = {
        [queryId]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [],
          tags: {},
          groupBy: {tags: [], time: null},
        },
      }
      const action = groupByTag(queryId, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].groupBy).to.eql({
        time: null,
        tags: ['k1'],
      })
    })

    it('removes a tag if the given tag key is already in the GROUP BY list', () => {
      const initialState = {
        [queryId]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [],
          tags: {},
          groupBy: {tags: ['k1'], time: null},
        },
      }
      const action = groupByTag(queryId, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].groupBy).to.eql({
        time: null,
        tags: [],
      })
    })
  })

  describe('DE_TOGGLE_TAG_ACCEPTANCE', () => {
    it('it toggles areTagsAccepted', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }
      const action = toggleTagAcceptance(queryId)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].areTagsAccepted).to.equal(
        !initialState[queryId].areTagsAccepted
      )
    })
  })

  describe('DE_GROUP_BY_TIME', () => {
    it('applys the appropriate group by time', () => {
      const time = '100y'
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }

      const action = groupByTime(queryId, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].groupBy.time).to.equal(time)
    })
  })

  it('updates entire config', () => {
    const initialState = {
      [queryId]: buildInitialState(queryId),
    }
    const expected = defaultQueryConfig({id: queryId}, {rawText: 'hello'})
    const action = updateQueryConfig(expected)

    const nextState = reducer(initialState, action)

    expect(nextState[queryId]).to.deep.equal(expected)
  })

  it("updates a query's raw text", () => {
    const initialState = {
      [queryId]: buildInitialState(queryId),
    }
    const text = 'foo'
    const action = updateRawQuery(queryId, text)

    const nextState = reducer(initialState, action)

    expect(nextState[queryId].rawText).to.equal('foo')
  })

  it("updates a query's raw status", () => {
    const initialState = {
      [queryId]: buildInitialState(queryId),
    }
    const status = 'your query was sweet'
    const action = editQueryStatus(queryId, status)

    const nextState = reducer(initialState, action)

    expect(nextState[queryId].status).to.equal(status)
  })

  describe('DE_FILL', () => {
    it('applies an explicit fill when group by time is used', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }
      const time = '10s'
      const action = groupByTime(queryId, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fill).to.equal(NULL_STRING)
    })

    it('updates fill to non-null-string non-number string value', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }
      const action = fill(queryId, LINEAR)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fill).to.equal(LINEAR)
    })

    it('updates fill to string integer value', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }
      const INT_STRING = '1337'
      const action = fill(queryId, INT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fill).to.equal(INT_STRING)
    })

    it('updates fill to string float value', () => {
      const initialState = {
        [queryId]: buildInitialState(queryId),
      }
      const FLOAT_STRING = '1.337'
      const action = fill(queryId, FLOAT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fill).to.equal(FLOAT_STRING)
    })
  })
})
