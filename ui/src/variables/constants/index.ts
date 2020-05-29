export const OPTION_NAME = 'v'
export const TIME_RANGE_START = 'timeRangeStart'
export const TIME_RANGE_STOP = 'timeRangeStop'
export const WINDOW_PERIOD = 'windowPeriod'

const START = '^'
const QUOTE = '"'
const SPACE = '\\s'
const NEWLINE = '\\n'
const COMMENT = '\\/\\/'
const ADD = '\\+'
const SUB = '\\-'
const MUL = '\\*'
const DIV = '\\/'
const MOD = '\\%'
const EQ = '\\=\\='
const LT = '\\<'
const GT = '\\>'
const LTE = '\\<='
const GTE = '\\>\\='
const NEQ = '\\!\\='
const REGEXEQ = '\\=\\~'
const REGEXNEQ = '\\!\\~'
const ASSIGN = '\\='
const ARROW = '\\=\\>'
const LPAREN = '\\('
const RPAREN = '\\)'
const LBRACK = '\\['
const RBRACK = '\\]'
const LBRACE = '\\{'
const RBRACE = '\\}'
const COLON = '\\:'
const COMMA = '\\,'
const EOF = '$'

const FLUX_BOUNDARY = [
  START,
  QUOTE,
  SPACE,
  NEWLINE,
  COMMENT,
  // and, or, and not all require spaces
  ADD,
  SUB,
  MUL,
  DIV,
  MOD,
  EQ,
  LT,
  GT,
  LTE,
  GTE,
  NEQ,
  REGEXEQ,
  REGEXNEQ,
  ASSIGN,
  ARROW,
  LPAREN,
  RPAREN,
  LBRACK,
  RBRACK,
  LBRACE,
  RBRACE,
  COLON,
  COMMA,
  EOF,
].join('|')

export const BOUNDARY_GROUP = `(${FLUX_BOUNDARY})`

export const variableItemTypes = [
  {
    type: 'map',
    label: 'Map',
  },
  {
    type: 'query',
    label: 'Query',
  },
  {
    type: 'constant',
    label: 'CSV',
  },
]
