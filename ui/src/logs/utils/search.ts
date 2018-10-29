import uuid from 'uuid'
import _ from 'lodash'

import {Filter} from 'src/types/logs'
import {
  Term,
  TermPart,
  TermRule,
  TermType,
  Operator,
  TokenLiteralMatch,
} from 'src/types/logs'

const MESSAGE_KEY = 'message'
const APP_NAME = 'appname'

export const createRule = (
  part: TermPart,
  type: TermType = TermType.Include
): TermRule => ({
  type,
  pattern: getPattern(type, part),
})

const getPattern = (type: TermType, phrase: TermPart): RegExp => {
  const {Attribute, Colon, Exclusion} = TermPart
  const phrasePattern = `(${Attribute}${Colon})?${phrase}`

  switch (type) {
    case TermType.Exclude:
      return new RegExp(`^${Exclusion}${phrasePattern}`)
    default:
      return new RegExp(`^${phrasePattern}`)
  }
}

export const LOG_SEARCH_TERMS: TermRule[] = [
  createRule(TermPart.SingleQuoted, TermType.Exclude),
  createRule(TermPart.DoubleQuoted, TermType.Exclude),
  createRule(TermPart.SingleQuoted),
  createRule(TermPart.DoubleQuoted),
  createRule(TermPart.UnquotedWord, TermType.Exclude),
  createRule(TermPart.UnquotedWord),
]

export const searchToFilters = (searchTerm: string): Filter[] => {
  const allTerms = extractTerms(searchTerm, LOG_SEARCH_TERMS)

  return termsToFilters(allTerms)
}

const termsToFilters = (terms: Term[]): Filter[] => {
  return terms.map(t => createAttributeFilter(t.attribute, t.term, termToOp(t)))
}

const extractTerms = (searchTerms: string, rules: TermRule[]): Term[] => {
  let tokens = []
  let text = searchTerms.trim()

  while (!_.isEmpty(text)) {
    const {nextTerm, nextText} = extractNextTerm(text, rules)
    tokens = [...tokens, nextTerm]
    text = nextText
  }

  return tokens
}

const extractNextTerm = (text, rules: TermRule[]) => {
  const {literal, rule, nextText, attribute} = readToken(eatSpaces(text), rules)

  const nextTerm = createTerm(rule.type, literal, attribute)

  return {nextText, nextTerm}
}

const eatSpaces = (text: string): string => {
  return text.trim()
}

const readToken = (text: string, rules: TermRule[]): TokenLiteralMatch => {
  const rule = rules.find(r => text.match(new RegExp(r.pattern)) !== null)

  const term = new RegExp(rule.pattern).exec(text)
  const literal = term[3]
  const attribute = term[2]
  // differs from literal length because of quote and exclusion removal
  const termLength = term[0].length
  const nextText = text.slice(termLength)

  return {literal, nextText, rule, attribute}
}

const createTerm = (
  type: TermType,
  term: string,
  attribute: string = MESSAGE_KEY
): Term => ({
  type,
  term,
  attribute: getAttributeAliasName(attribute),
})

const createAttributeFilter = (
  key: string,
  value: string,
  operator: Operator
) => ({
  id: uuid.v4(),
  key,
  value,
  operator,
})

const termToOp = (term: Term): Operator => {
  switch (term.attribute) {
    case MESSAGE_KEY:
    case APP_NAME:
      return handleOpExclusion(term, Operator.Like, Operator.NotLike)
    default:
      return handleOpExclusion(term, Operator.Equal, Operator.NotEqual)
  }
}

const handleOpExclusion = (
  term: Term,
  inclusion: Operator,
  exclusion: Operator
): Operator => {
  switch (term.type) {
    case TermType.Exclude:
      return exclusion
    case TermType.Include:
      return inclusion
  }
}

export const getAttributeAliasName = (name: string) => {
  const lowerName = name.toLowerCase()

  switch (lowerName) {
    case 'ap':
    case 'app':
    case 'apps':
    case 'application':
    case 'program':
      return APP_NAME
    default:
      return name
  }
}
