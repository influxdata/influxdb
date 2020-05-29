import {
  SearchExpr,
  SearchBooleanOperator,
  SearchTagExpr,
  SearchTagOperator,
  SearchBooleanExpr,
  SearchStringLiteral,
  SearchRegexLiteral,
} from 'src/eventViewer/types'

/*
  Parse search input into an AST.

  For example, given this search input:

      "foo" == "bar" and "baz" != "foo"

  We would obtain the following AST:

      {
        type: 'BooleanExpression',
        operator: 'and',
        left: {
          type: 'BooleanExpression',
          operator: '==',
          left: {type: 'StringLiteral', value: 'foo'},
          right: {type: 'StringLiteral', value: 'bar'},
        },
        right: {
          type: 'BooleanExpression',
          operator: '!=',
          left: {type: 'StringLiteral', value: 'baz'},
          right: {type: 'StringLiteral', value: 'foo'},
        },
      }


  Empty input will return `null`. If the input cannot be parsed, an exception
  will be thrown.

  Currently the search grammar is (roughly) a subset of the Flux grammar,
  though I expect that to change in the future.
*/
export const parseSearchInput = (input: string): SearchExpr | null => {
  if (input.trim() === '') {
    return null
  }

  return new Parser(input).parse()
}

/*
  Format the AST for a search term into a Flux boolean expression that can be
  used as a predicate in a `filter` call.

  For example, the AST:

      {
        type: 'BooleanExpression',
        operator: '==',
        left: {type: 'StringLiteral', value: 'foo'},
        right: {type: 'StringLiteral', value: 'bar'},
      }

  Would be printed as the Flux expression:

      r["foo"] == "bar"

  Which is intended to be used in a Flux filter call like so:

      // ...
      |> filter(fn: (r) => r["foo"] == "bar")

*/
export const searchExprToFlux = (searchExpr: SearchExpr | null): string => {
  if (searchExpr === null) {
    return 'true'
  }

  switch (searchExpr.type) {
    case 'BooleanExpression': {
      const left = searchExprToFlux(searchExpr.left)
      const right = searchExprToFlux(searchExpr.right)

      return `(${left}) ${searchExpr.operator} (${right})`
    }

    case 'TagExpression': {
      const left = searchExpr.left.value
      const right = searchExpr.right.value
      const delim = searchExpr.right.type === 'RegexLiteral' ? '/' : '"'

      return `r["${left}"] ${searchExpr.operator} ${delim}${right}${delim}`
    }

    default: {
      const badExpr = JSON.stringify(searchExpr)

      throw new Error(`cannot convert search expression to Flux: ${badExpr} `)
    }
  }
}

export const isSearchInputValid = (input: string): boolean => {
  try {
    parseSearchInput(input)
  } catch {
    return false
  }

  return true
}

type Token =
  | SearchStringLiteral
  | SearchRegexLiteral
  | {type: '('}
  | {type: ')'}
  | {type: 'and'}
  | {type: 'or'}
  | {type: '=='}
  | {type: '!='}
  | {type: '=~'}
  | {type: '!~'}

class Lexer {
  private input: string
  private i: number = 0

  constructor(input) {
    this.input = input
  }

  public lex(): Token[] {
    const tokens: Token[] = []

    while (this.current()) {
      switch (this.current()) {
        case ' ':
        case '\r':
        case '\n':
          this.advance()
          break
        case '(':
          tokens.push({type: '('})
          this.advance()
          break
        case ')':
          tokens.push({type: ')'})
          this.advance()
          break
        case 'a':
          this.advance()
          this.expect('n')
          this.advance()
          this.expect('d')
          tokens.push({type: 'and'})
          this.advance()
          break
        case 'o':
          this.advance()
          this.expect('r')
          tokens.push({type: 'or'})
          this.advance()
          break
        case '=':
        case '!':
          const head = this.current()
          this.advance()
          this.expect('=', '~')
          tokens.push({type: `${head}${this.current()}`} as Token)
          this.advance()
          break
        case '"':
          this.advance()
          tokens.push({type: 'StringLiteral', value: this.readToDelimiter('"')})
          this.advance()
          break
        case '/':
          this.advance()
          tokens.push({type: 'RegexLiteral', value: this.readToDelimiter('/')})
          this.advance()
          break
        default:
          throw new Error(`unexpected character "${this.current()}"`)
      }
    }

    return tokens
  }

  private readToDelimiter(delimiter: string): string {
    let value = ''

    while (this.current() !== delimiter) {
      if (!this.current()) {
        throw new Error('unexpected end of input')
      }

      value += this.current()
      this.advance()
    }

    return value
  }

  private current() {
    return this.input[this.i]
  }

  private advance() {
    this.i += 1

    return this.input[this.i]
  }

  private expect(...expected: string[]) {
    const actual = this.input[this.i]

    if (!expected.includes(actual)) {
      throw new Error(
        `expected one of "${JSON.stringify(expected)}" but got "${actual}"`
      )
    }

    return actual
  }
}

class Parser {
  private input: string
  private i: number = 0
  private tokens: Token[]

  private static PRECEDENCES = {
    ')': 0,
    and: 1,
    or: 1,
    '==': 2,
    '!=': 2,
    '=~': 2,
    '!~': 2,
    '(': 3,
  }

  constructor(input) {
    this.input = input
  }

  public parse(): SearchExpr {
    this.tokens = new Lexer(this.input).lex()

    return this.parseExpr()
  }

  private parseExpr(precedence = 0): SearchExpr {
    let left

    switch (this.current().type) {
      case 'StringLiteral':
        left = this.parseStringLiteral()
        break
      case 'RegexLiteral':
        left = this.parseRegexLiteral()
        break

      case '(':
        left = this.parseGroupedExpr()
        break

      default:
        throw new Error(`unexpected token ${this.current().type}`)
    }

    while (!!this.next() && precedence < this.nextPrecedence()) {
      this.advance()

      switch (left.type) {
        case 'StringLiteral':
          left = this.parseTagExpr(left)
          break

        case 'BooleanExpression':
        case 'TagExpression':
          left = this.parseBooleanExpr(left)
          break

        default:
          throw new Error(`unexpected node ${left.type}`)
      }
    }

    return left
  }

  private parseBooleanExpr(left): SearchBooleanExpr {
    this.expect('and', 'or')
    const operator = this.current().type as SearchBooleanOperator
    const precedence = this.currentPrecedence()

    this.advance()
    const right = this.parseExpr(precedence)

    return {type: 'BooleanExpression', operator, left, right}
  }

  private parseTagExpr(left): SearchTagExpr {
    this.expect('==', '!=', '=~', '!~')
    const operator = this.current().type as SearchTagOperator

    this.advance()
    const right = operator.endsWith('~')
      ? this.parseRegexLiteral()
      : this.parseStringLiteral()

    return {type: 'TagExpression', operator, left, right}
  }

  private parseStringLiteral(): SearchStringLiteral {
    this.expect('StringLiteral')

    return this.current() as SearchStringLiteral
  }

  private parseRegexLiteral(): SearchRegexLiteral {
    this.expect('RegexLiteral')

    return this.current() as SearchRegexLiteral
  }

  private parseGroupedExpr(): SearchExpr {
    this.advance()
    const expr = this.parseExpr(0)
    this.advance()
    this.expect(')')
    return expr
  }

  private advance(): void {
    this.i += 1
  }

  private current(): Token {
    return this.tokens[this.i]
  }

  private next(): Token {
    return this.tokens[this.i + 1]
  }

  private currentPrecedence(): number {
    return Parser.PRECEDENCES[this.tokens[this.i].type]
  }

  private nextPrecedence(): number {
    return Parser.PRECEDENCES[this.tokens[this.i + 1].type]
  }

  private expect(...expectedTypes: Token['type'][]) {
    const expectedMsg = `expected one of ${JSON.stringify(expectedTypes)} `

    if (!this.current()) {
      throw new Error(`${expectedMsg} but found nothing`)
    }

    const actual = this.current().type

    if (!expectedTypes.includes(actual)) {
      throw new Error(`${expectedMsg} but got ${actual}`)
    }
  }
}
