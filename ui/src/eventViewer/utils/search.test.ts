import {parseSearchInput, searchExprToFlux} from './search'

const SUCCESS_TEST_CASES = [
  [
    'basic == expression',
    `"foo" == "bar"`,
    {
      type: 'TagExpression',
      operator: '==',
      left: {type: 'StringLiteral', value: 'foo'},
      right: {type: 'StringLiteral', value: 'bar'},
    },
    'r["foo"] == "bar"',
  ],
  [
    'basic != expression',
    `"foo" != "bar"`,
    {
      type: 'TagExpression',
      operator: '!=',
      left: {type: 'StringLiteral', value: 'foo'},
      right: {type: 'StringLiteral', value: 'bar'},
    },
    'r["foo"] != "bar"',
  ],
  [
    'basic =~ expression',
    `"foo" =~ /bar/`,
    {
      type: 'TagExpression',
      operator: '=~',
      left: {type: 'StringLiteral', value: 'foo'},
      right: {type: 'RegexLiteral', value: 'bar'},
    },
    'r["foo"] =~ /bar/',
  ],
  [
    'basic !~ expression',
    `"foo" !~ /bar/`,
    {
      type: 'TagExpression',
      operator: '!~',
      left: {type: 'StringLiteral', value: 'foo'},
      right: {type: 'RegexLiteral', value: 'bar'},
    },
    'r["foo"] !~ /bar/',
  ],
  [
    'realistic expression with and/or operators and precedence',
    `("foo" == "bar" or "bar" == "baz") and "florp" != "snorp"`,
    {
      type: 'BooleanExpression',
      operator: 'and',
      left: {
        type: 'BooleanExpression',
        operator: 'or',
        left: {
          type: 'TagExpression',
          operator: '==',
          left: {type: 'StringLiteral', value: 'foo'},
          right: {type: 'StringLiteral', value: 'bar'},
        },
        right: {
          type: 'TagExpression',
          operator: '==',
          left: {type: 'StringLiteral', value: 'bar'},
          right: {type: 'StringLiteral', value: 'baz'},
        },
      },
      right: {
        type: 'TagExpression',
        operator: '!=',
        left: {type: 'StringLiteral', value: 'florp'},
        right: {type: 'StringLiteral', value: 'snorp'},
      },
    },
    '((r["foo"] == "bar") or (r["bar"] == "baz")) and (r["florp"] != "snorp")',
  ],
  [
    'nested expression',
    '"e" == "f" or (("a" == "b") and ("c" == "d")) or "g" == "h"',
    {
      type: 'BooleanExpression',
      operator: 'or',
      left: {
        type: 'BooleanExpression',
        operator: 'or',
        left: {
          type: 'TagExpression',
          operator: '==',
          left: {type: 'StringLiteral', value: 'e'},
          right: {type: 'StringLiteral', value: 'f'},
        },
        right: {
          type: 'BooleanExpression',
          operator: 'and',
          left: {
            type: 'TagExpression',
            operator: '==',
            left: {type: 'StringLiteral', value: 'a'},
            right: {type: 'StringLiteral', value: 'b'},
          },
          right: {
            type: 'TagExpression',
            operator: '==',
            left: {type: 'StringLiteral', value: 'c'},
            right: {type: 'StringLiteral', value: 'd'},
          },
        },
      },
      right: {
        left: {type: 'StringLiteral', value: 'g'},
        operator: '==',
        right: {type: 'StringLiteral', value: 'h'},
        type: 'TagExpression',
      },
    },
    '((r["e"] == "f") or ((r["a"] == "b") and (r["c"] == "d"))) or (r["g"] == "h")',
  ],
  ['empty expression', '', null, 'true'],
  ['empty expression with whitespace', '     ', null, 'true'],
]

const FAIL_TEST_CASES = ['howdy', '() and ()', '/bar/ =~ /bar/']

describe('event viewer search utilities', () => {
  describe('parseSearchInput', () => {
    test.each(SUCCESS_TEST_CASES)('%p', (_, input, expected) => {
      expect(parseSearchInput(input)).toEqual(expected)
    })

    test.each(FAIL_TEST_CASES)("fails to parse input '%p'", input => {
      expect(() => parseSearchInput(input)).toThrowError()
    })
  })

  describe('searchExprToFlux', () => {
    test.each(SUCCESS_TEST_CASES)(
      'can format expression for: %p',
      (_, __, expr, expected) => {
        expect(searchExprToFlux(expr)).toEqual(expected)
      }
    )
  })
})
