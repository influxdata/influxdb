import reducer from 'src/data_explorer/reducers/queryConfigs'
import defaultQueryConfig from 'src/utils/defaultQueryConfig'
import {
  chooseNamespace,
  chooseMeasurement,
  toggleField,
  applyFuncsToField,
  chooseTag,
  groupByTag,
  groupByTime,
  toggleTagAcceptance,
  fill,
  updateQueryConfig,
  updateRawQuery,
  editQueryStatus,
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

  describe.only('a query has measurements and fields', () => {
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
        toggleField(queryId, {
          name: 'a great field',
          alias: null,
          args: [],
          type: 'field',
        })
      )
    })

    describe('choosing a new namespace', () => {
      it('clears out the old measurement and fields', () => {
        // what about tags?
        expect(state[queryId].measurement).to.exist
        expect(state[queryId].fields.length).to.equal(1)

        const newState = reducer(
          state,
          chooseNamespace(queryId, {
            database: 'newdb',
            retentionPolicy: 'newrp',
          })
        )

        expect(newState[queryId].measurement).not.to.exist
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
          toggleField(queryId, {field: 'a different field', funcs: []})
        )

        expect(newState[queryId].fields.length).to.equal(2)
        expect(newState[queryId].fields[1].field).to.equal('a different field')
      })

      it('applies a funcs to newly selected fields', () => {
        expect(state[queryId].fields.length).to.equal(1)

        const oneFieldOneFunc = reducer(
          state,
          applyFuncsToField(queryId, {
            name: 'mean',
            alias: `mean_usage_user`,
            args: ['usage_user'],
            type: 'func',
          })
        )

        const newState = reducer(
          oneFieldOneFunc,
          toggleField(queryId, {
            name: 'f1',
            alias: null,
            args: [],
            type: 'field',
          })
        )

        expect(newState[queryId].fields[1].name).to.equal('mean')
        expect(newState[queryId].fields[1].alias).to.equal('mean_f1')
        expect(newState[queryId].fields[1].args).to.deep.equal(['f1'])
        expect(newState[queryId].fields[1].type).to.equal('func')
        expect(newState[queryId].fields[1].type).to.equal('field')
      })

      it('adds the field property to query config if not found', () => {
        delete state[queryId].fields
        expect(state[queryId].fields).to.equal(undefined)

        const field = 'fk1'
        const newState = reducer(
          state,
          toggleField(queryId, {field: 'fk1', funcs: []})
        )

        expect(newState[queryId].fields.length).to.equal(1)
        expect(newState[queryId].fields[0].field).to.equal(field)
      })
    })
  })

  describe('DE_APPLY_FUNCS_TO_FIELD', () => {
    it('applies functions to a field without any existing functions', () => {
      const initialState = {
        [queryId]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [
            {field: 'f1', funcs: ['fn1', 'fn2']},
            {field: 'f2', funcs: ['fn1']},
          ],
        },
      }
      const action = applyFuncsToField(queryId, {
        field: 'f1',
        funcs: ['fn3', 'fn4'],
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fields).to.eql([
        {field: 'f1', funcs: ['fn3', 'fn4']},
        {field: 'f2', funcs: ['fn1']},
      ])
    })

    it('removes all functions and group by time when one field has no funcs applied', () => {
      const initialState = {
        [queryId]: {
          id: 123,
          database: 'db1',
          measurement: 'm1',
          fields: [
            {field: 'f1', funcs: ['fn1', 'fn2']},
            {field: 'f2', funcs: ['fn3', 'fn4']},
          ],
          groupBy: {
            time: '1m',
            tags: [],
          },
        },
      }

      const action = applyFuncsToField(queryId, {
        field: 'f1',
        funcs: [],
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryId].fields).to.eql([
        {field: 'f1', funcs: []},
        {field: 'f2', funcs: []},
      ])
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
