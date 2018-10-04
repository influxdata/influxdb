import {QueryConfig, Namespace, Source} from 'src/types'

import {TimeRange} from 'src/types/logs'

export enum SearchStatus {
  None = 'None',
  Loading = 'Loading',
  NoResults = 'NoResults',
  UpdatingTimeBounds = 'UpdatingTimeBounds',
  UpdatingFilters = 'UpdatingFilters',
  UpdatingSource = 'UpdatingSource',
  UpdatingNamespace = 'UpdatingNamespace',
  SourceError = 'SourceError',
  Loaded = 'Loaded',
  Clearing = 'Clearing',
  Cleared = 'Cleared',
}

export interface Filter {
  id: string
  key: string
  value: string
  operator: string
}

export interface LogsState {
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace | null
  tableQueryConfig: QueryConfig | null
  filters: Filter[]
  queryCount: number
  logConfig: LogConfig
  searchStatus: SearchStatus
}

export interface LogConfig {
  isTruncated: boolean
}

export interface TimeWindow {
  seconds: number
  windowOption: string
}

export interface TimeRange {
  upper?: string
  lower: string
  seconds?: number
  windowOption: string
  timeOption: string
}

export interface Term {
  type: TermType
  term: string
  attribute: string
}

export interface TokenLiteralMatch {
  literal: string
  nextText: string
  rule: TermRule
  attribute: string
}

export interface TermRule {
  type: TermType
  pattern: RegExp
}

export enum TermType {
  EXCLUDE,
  INCLUDE,
}

export enum TermPart {
  EXCLUSION = '-',
  SINGLE_QUOTED = "'([^']+)'",
  DOUBLE_QUOTED = '"([^"]+)"',
  ATTRIBUTE = '(\\w+(?=\\:))',
  COLON = '(?::)',
  UNQUOTED_WORD = '([\\S]+)',
}

export enum Operator {
  NOT_LIKE = '!~',
  LIKE = '=~',
  EQUAL = '==',
  NOT_EQUAL = '!=',
}

export enum MatchType {
  NONE = 'no-match',
  MATCH = 'match',
}

export interface MatchSection {
  id: string
  type: MatchType
  text: string
}
