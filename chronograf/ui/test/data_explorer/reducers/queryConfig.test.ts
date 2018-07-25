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
  editRawText,
  updateRawQuery,
  editQueryStatus,
  chooseNamespace,
  chooseMeasurement,
  applyFuncsToField,
  addInitialField,
  updateQueryConfig,
  toggleTagAcceptance,
  ActionAddQuery,
} from 'src/data_explorer/actions/view'

import {LINEAR, NULL_STRING} from 'src/shared/constants/queryFillOptions'

const fakeAddQueryAction = (queryID: string): ActionAddQuery => {
  return {
    type: 'DE_ADD_QUERY',
    payload: {
      queryID,
    },
  }
}

function buildInitialState(queryID, params?) {
  return {
    ...defaultQueryConfig({
      id: queryID,
    }),
    ...params,
  }
}

describe('Chronograf.Reducers.DataExplorer.queryConfigs', () => {
  const queryID = '123'

  it('can add a query', () => {
    const state = reducer({}, fakeAddQueryAction(queryID))

    const actual = state[queryID]
    const expected = defaultQueryConfig({
      id: queryID,
    })
    expect(actual).toEqual(expected)
  })

  describe('choosing db, rp, and measurement', () => {
    let state
    beforeEach(() => {
      state = reducer({}, fakeAddQueryAction(queryID))
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
      const one = reducer({}, fakeAddQueryAction(queryID))
      const two = reducer(
        one,
        chooseNamespace(queryID, {
          database: '_internal',
          retentionPolicy: 'daily',
        })
      )
      const three = reducer(two, chooseMeasurement(queryID, 'disk'))
      const field = {
        value: 'a great field',
        type: 'field',
      }
      const groupBy = {}

      state = reducer(three, addInitialField(queryID, field, groupBy))
    })

    describe('choosing a new namespace', () => {
      it('clears out the old measurement and fields', () => {
        // what about tags?
        expect(state[queryID].measurement).toBe('disk')
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          chooseNamespace(queryID, {
            database: 'newdb',
            retentionPolicy: 'newrp',
          })
        )

        expect(newState[queryID].measurement).toBe(null)
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

    describe('DE_TOGGLE_FIELD', () => {
      it('can toggle multiple fields', () => {
        expect(state[queryID].fields.length).toBe(1)

        const newState = reducer(
          state,
          toggleField(queryID, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryID].fields.length).toBe(2)
        expect(newState[queryID].fields[1].alias).toEqual('mean_f2')
        expect(newState[queryID].fields[1].args).toEqual([
          {
            value: 'f2',
            type: 'field',
          },
        ])
        expect(newState[queryID].fields[1].value).toEqual('mean')
      })

      it('applies a func to newly selected fields', () => {
        expect(state[queryID].fields.length).toBe(1)
        expect(state[queryID].fields[0].type).toBe('func')
        expect(state[queryID].fields[0].value).toBe('mean')

        const newState = reducer(
          state,
          toggleField(queryID, {
            value: 'f2',
            type: 'field',
          })
        )

        expect(newState[queryID].fields[1].value).toBe('mean')
        expect(newState[queryID].fields[1].alias).toBe('mean_f2')
        expect(newState[queryID].fields[1].args).toEqual([
          {
            value: 'f2',
            type: 'field',
          },
        ])
        expect(newState[queryID].fields[1].type).toBe('func')
      })

      it('adds the field property to query config if not found', () => {
        delete state[queryID].fields
        expect(state[queryID].fields).toBe(undefined)

        const newState = reducer(
          state,
          toggleField(queryID, {
            value: 'fk1',
            type: 'field',
          })
        )

        expect(newState[queryID].fields.length).toBe(1)
      })
    })
  })

  describe('DE_APPLY_FUNCS_TO_FIELD', () => {
    it('applies new functions to a field', () => {
      const f1 = {
        value: 'f1',
        type: 'field',
      }
      const f2 = {
        value: 'f2',
        type: 'field',
      }

      const initialState = {
        [queryID]: buildInitialState(queryID, {
          id: '123',
          database: 'db1',
          measurement: 'm1',
          fields: [
            {
              value: 'fn1',
              type: 'func',
              args: [f1],
              alias: `fn1_${f1.value}`,
            },
            {
              value: 'fn1',
              type: 'func',
              args: [f2],
              alias: `fn1_${f2.value}`,
            },
            {
              value: 'fn2',
              type: 'func',
              args: [f1],
              alias: `fn2_${f1.value}`,
            },
          ],
        }),
      }

      const action = applyFuncsToField(queryID, {
        field: {
          value: 'f1',
          type: 'field',
        },
        funcs: [
          {
            value: 'fn3',
            type: 'func',
          },
          {
            value: 'fn4',
            type: 'func',
          },
        ],
      })

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fields).toEqual([
        {
          value: 'fn3',
          type: 'func',
          args: [f1],
          alias: `fn3_${f1.value}`,
        },
        {
          value: 'fn4',
          type: 'func',
          args: [f1],
          alias: `fn4_${f1.value}`,
        },
        {
          value: 'fn1',
          type: 'func',
          args: [f2],
          alias: `fn1_${f2.value}`,
        },
      ])
    })
  })

  describe('DE_REMOVE_FUNCS', () => {
    it('removes all functions and group by time when one field has no funcs applied', () => {
      const f1 = {
        value: 'f1',
        type: 'field',
      }
      const f2 = {
        value: 'f2',
        type: 'field',
      }
      const fields = [
        {
          value: 'fn1',
          type: 'func',
          args: [f1],
          alias: `fn1_${f1.value}`,
        },
        {
          value: 'fn1',
          type: 'func',
          args: [f2],
          alias: `fn1_${f2.value}`,
        },
      ]
      const groupBy = {
        time: '1m',
        tags: [],
      }

      const initialState = {
        [queryID]: buildInitialState(queryID, {
          id: '123',
          database: 'db1',
          measurement: 'm1',
          fields,
          groupBy,
        }),
      }

      const action = removeFuncs(queryID, fields, groupBy)

      const nextState = reducer(initialState, action)
      const actual = nextState[queryID].fields
      const expected = [f1, f2]

      expect(actual).toEqual(expected)
      expect(nextState[queryID].groupBy.time).toBe(null)
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

  describe('DE_GROUP_BY_TAG', () => {
    it('adds a tag key/value to the query', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID, {
          id: '123',
          database: 'db1',
          measurement: 'm1',
          fields: [],
          tags: {},
          groupBy: {
            tags: [],
            time: null,
          },
        }),
      }
      const action = groupByTag(queryID, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy).toEqual({
        time: null,
        tags: ['k1'],
      })
    })

    it('removes a tag if the given tag key is already in the GROUP BY list', () => {
      const query = {
        id: '123',
        database: 'db1',
        measurement: 'm1',
        fields: [],
        tags: {},
        groupBy: {
          tags: ['k1'],
          time: null,
        },
      }

      const initialState = {
        [queryID]: buildInitialState(queryID, query),
      }
      const action = groupByTag(queryID, 'k1')

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].groupBy).toEqual({
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

      expect(nextState[queryID].areTagsAccepted).toBe(
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

      expect(nextState[queryID].groupBy.time).toBe(time)
    })
  })

  it('updates entire config', () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const id = {id: queryID}
    const expected = defaultQueryConfig(id)
    const action = updateQueryConfig(expected)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID]).toEqual(expected)
  })

  it("updates a query's raw text", () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const text = 'foo'
    const action = updateRawQuery(queryID, text)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID].rawText).toBe('foo')
  })

  it("updates a query's raw status", () => {
    const initialState = {
      [queryID]: buildInitialState(queryID),
    }
    const status = {success: 'Your query was very nice'}
    const action = editQueryStatus(queryID, status)

    const nextState = reducer(initialState, action)

    expect(nextState[queryID].status).toEqual(status)
  })

  describe('DE_FILL', () => {
    it('applies an explicit fill when group by time is used', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const time = '10s'
      const action = groupByTime(queryID, time)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).toBe(NULL_STRING)
    })

    it('updates fill to non-null-string non-number string value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const action = fill(queryID, LINEAR)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).toBe(LINEAR)
    })

    it('updates fill to string integer value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const INT_STRING = '1337'
      const action = fill(queryID, INT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).toBe(INT_STRING)
    })

    it('updates fill to string float value', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }
      const FLOAT_STRING = '1.337'
      const action = fill(queryID, FLOAT_STRING)

      const nextState = reducer(initialState, action)

      expect(nextState[queryID].fill).toBe(FLOAT_STRING)
    })
  })

  describe('DE_TIME_SHIFT', () => {
    it('can shift the time', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const shift = {
        quantity: '1',
        unit: 'd',
        duration: '1d',
        label: 'label',
      }

      const action = timeShift(queryID, shift)
      const nextState = reducer(initialState, action)

      expect(nextState[queryID].shifts).toEqual([shift])
    })
  })

  describe('DE_EDIT_RAW_TEXT', () => {
    it('can edit the raw text', () => {
      const initialState = {
        [queryID]: buildInitialState(queryID),
      }

      const rawText = 'im the raw text'
      const action = editRawText(queryID, rawText)
      const nextState = reducer(initialState, action)

      expect(nextState[queryID].rawText).toEqual(rawText)
    })
  })
})
