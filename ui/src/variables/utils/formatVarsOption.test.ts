import {VariableAssignment} from 'src/types/ast'
import {formatVarsOption} from 'src/variables/utils/formatVarsOption'

describe('formatVarsOption', () => {
  test('should return an empty string if passed no variables', () => {
    const variables = []
    const actual = formatVarsOption(variables)

    expect(actual).toEqual('')
  })

  test('should be able to format different expression types', () => {
    // This tests the following expression types:
    //
    // - DateTimeLiteral
    // - StringLiteral
    // - DurationLiteral
    // - BooleanLiteral
    // - UnsignedIntegerLiteral
    // - IntegerLiteral
    // - FloatLiteral
    // - UnaryExpression
    // - BinaryExpression
    // - CallExpression
    const variables: VariableAssignment[] = [
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'a',
        },
        init: {
          type: 'UnaryExpression',
          operator: '-',
          argument: {
            type: 'DurationLiteral',
            values: [
              {magnitude: 99, unit: 'mo'},
              {magnitude: 1, unit: 'ns'},
            ],
          },
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'b',
        },
        init: {
          type: 'BinaryExpression',
          operator: '-',
          left: {
            type: 'DateTimeLiteral',
            value: '2010-01-01T00:00:00Z',
          },
          right: {
            type: 'DurationLiteral',
            values: [
              {magnitude: 99, unit: 'mo'},
              {magnitude: 1, unit: 'ns'},
            ],
          },
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'c',
        },
        init: {
          type: 'CallExpression',
          callee: {
            type: 'Identifier',
            name: 'now',
          },
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'd',
        },
        init: {
          type: 'StringLiteral',
          value: 'howdy',
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'e',
        },
        init: {
          type: 'BooleanLiteral',
          value: false,
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'f',
        },
        init: {
          type: 'UnsignedIntegerLiteral',
          value: 40,
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'g',
        },
        init: {
          type: 'IntegerLiteral',
          value: 40,
        },
      },
      {
        type: 'VariableAssignment',
        id: {
          type: 'Identifier',
          name: 'h',
        },
        init: {
          type: 'FloatLiteral',
          value: 40,
        },
      },
    ]

    const actual = formatVarsOption(variables)

    expect(actual).toEqual(
      `option v = {
  a: -99mo1ns,
  b: 2010-01-01T00:00:00Z - 99mo1ns,
  c: now(),
  d: "howdy",
  e: false,
  f: 40,
  g: 40,
  h: 40.0
}`
    )
  })
})
