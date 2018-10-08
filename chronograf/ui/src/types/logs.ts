import {QueryConfig, Namespace, Source} from 'src/types'

import {FieldOption} from 'src/types/v2/dashboards'

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

// Log Config
export interface LogConfig {
  id?: string
  link?: string
  tableColumns: LogsTableColumn[]
  severityFormat: SeverityFormat
  severityLevelColors: SeverityLevelColor[]
  isTruncated: boolean
}

export interface SeverityLevelColor {
  level: SeverityLevelOptions
  color: SeverityColorOptions
}

export interface SeverityColor {
  hex: string
  name: SeverityColorOptions
}

export type SeverityFormat = SeverityFormatOptions

export type LogsTableColumn = FieldOption

// Log Severity
export enum SeverityLevelOptions {
  emerg = 'emerg',
  alert = 'alert',
  crit = 'crit',
  err = 'err',
  warning = 'warning',
  notice = 'notice',
  info = 'info',
  debug = 'debug',
}

export enum SeverityFormatOptions {
  dot = 'dot',
  dotText = 'dotText',
  text = 'text',
}

export enum SeverityColorOptions {
  ruby = 'ruby',
  fire = 'fire',
  curacao = 'curacao',
  tiger = 'tiger',
  pineapple = 'pineapple',
  thunder = 'thunder',
  sulfur = 'sulfur',
  viridian = 'viridian',
  rainforest = 'rainforest',
  honeydew = 'honeydew',
  ocean = 'ocean',
  pool = 'pool',
  laser = 'laser',
  planet = 'planet',
  star = 'star',
  comet = 'comet',
  graphite = 'graphite',
  wolf = 'wolf',
  mist = 'mist',
  pearl = 'pearl',
}

export const SeverityColorValues = {
  [SeverityColorOptions.ruby]: '#BF3D5E',
  [SeverityColorOptions.fire]: '#DC4E58',
  [SeverityColorOptions.curacao]: '#F95F53',
  [SeverityColorOptions.tiger]: '#F48D38',
  [SeverityColorOptions.pineapple]: '#FFB94A',
  [SeverityColorOptions.thunder]: '#FFD255',
  [SeverityColorOptions.sulfur]: '#FFE480',
  [SeverityColorOptions.viridian]: '#32B08C',
  [SeverityColorOptions.rainforest]: '#4ED8A0',
  [SeverityColorOptions.honeydew]: '#7CE490',
  [SeverityColorOptions.ocean]: '#4591ED',
  [SeverityColorOptions.pool]: '#22ADF6',
  [SeverityColorOptions.laser]: '#00C9FF',
  [SeverityColorOptions.planet]: '#513CC6',
  [SeverityColorOptions.star]: '#7A65F2',
  [SeverityColorOptions.comet]: '#9394FF',
  [SeverityColorOptions.graphite]: '#545667',
  [SeverityColorOptions.wolf]: '#8E91A1',
  [SeverityColorOptions.mist]: '#BEC2CC',
  [SeverityColorOptions.pearl]: '#E7E8EB',
}

// Log Column Settings
export enum ColumnSettingTypes {
  visibility = 'visibility',
  display = 'displayName',
  label = 'label',
  color = 'color',
}

export enum ColumnSettingLabelOptions {
  text = 'text',
  icon = 'icon',
}

export enum ColumnSettingVisibilityOptions {
  visible = 'visible',
  hidden = 'hidden',
}

// Time
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

// Log Search
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
