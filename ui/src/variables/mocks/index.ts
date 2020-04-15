// Types
import {Variable, RemoteDataState} from 'src/types'
import {VariableAssignment} from 'src/types/ast'

export const defaultVariableAssignments: VariableAssignment[] = [
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'timeRangeStart',
    },
    init: {
      type: 'UnaryExpression',
      operator: '-',
      argument: {
        type: 'DurationLiteral',
        values: [{magnitude: 1, unit: 'h'}],
      },
    },
  },
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'timeRangeStop',
    },
    init: {
      type: 'CallExpression',
      callee: {type: 'Identifier', name: 'now'},
    },
  },
  {
    type: 'VariableAssignment',
    id: {
      type: 'Identifier',
      name: 'createdVariable',
    },
    init: {
      type: 'StringLiteral',
      value: 'randomValue',
    },
  },
]

export const createVariable = (
  name: string,
  query: string,
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'query',
    values: {
      query,
      language: 'flux',
    },
  },
  status: RemoteDataState.Done,
})

export const createMapVariable = (
  name: string,
  map: {[key: string]: string} = {},
  selected?: string
): Variable => ({
  name,
  id: name,
  orgID: 'howdy',
  selected: selected ? [selected] : [],
  labels: [],
  arguments: {
    type: 'map',
    values: {...map},
  },
  status: RemoteDataState.Done,
})
