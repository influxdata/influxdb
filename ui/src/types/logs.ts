import {Index} from 'react-virtualized'

import {QueryConfig} from 'src/types'
import {Bucket, Source} from 'src/api'

import {FieldOption, TimeSeriesValue} from 'src/types/v2/dashboards'

export interface LogSearchParams {
  lower: string
  upper: string
  config: QueryConfig
  filters: Filter[]
}

export interface LogQuery extends LogSearchParams {
  source: Source
}

export enum SearchStatus {
  None = 'None',
  Loading = 'Loading',
  NoResults = 'NoResults',
  UpdatingTimeBounds = 'UpdatingTimeBounds',
  UpdatingFilters = 'UpdatingFilters',
  UpdatingSource = 'UpdatingSource',
  UpdatingBucket = 'UpdatingBucket',
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

export interface LogsConfigState {
  currentSource: Source | null
  currentBuckets: Bucket[]
  currentBucket: Bucket | null
  tableQueryConfig: QueryConfig | null
  filters: Filter[]
  queryCount: number
  logConfig: LogConfig
  searchStatus: SearchStatus
}

export interface LogsTableDataState {
  currentTailID: number | undefined
  currentOlderBatchID: string | undefined
  tableTime: TableTime
  currentTailUpperBound: number | undefined
  nextTailLowerBound: number | undefined
  nextOlderUpperBound: number | undefined
  nextOlderLowerBound: number | undefined
  olderChunkDurationMs: number
  tailChunkDurationMs: number
  tableQueryConfig: QueryConfig | null
  tableInfiniteData: {
    forward: TableData
    backward: TableData
  }
}

export type LogsState = LogsConfigState & LogsTableDataState

// Log Config
export interface LogConfig {
  id?: string
  link?: string
  tableColumns: LogsTableColumn[]
  severityFormat: SeverityFormat
  severityLevelColors: SeverityLevelColor[]
  isTruncated: boolean
}

// Severity Colors
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
  Emerg = 'emerg',
  Alert = 'alert',
  Crit = 'crit',
  Err = 'err',
  Warning = 'warning',
  Notice = 'notice',
  Info = 'info',
  Debug = 'debug',
}

export enum SeverityFormatOptions {
  Dot = 'dot',
  DotText = 'dotText',
  Text = 'text',
}

export enum SeverityColorOptions {
  Ruby = 'ruby',
  Fire = 'fire',
  Curacao = 'curacao',
  Tiger = 'tiger',
  Pineapple = 'pineapple',
  Thunder = 'thunder',
  Sulfur = 'sulfur',
  Viridian = 'viridian',
  Rainforest = 'rainforest',
  Honeydew = 'honeydew',
  Ocean = 'ocean',
  Pool = 'pool',
  Laser = 'laser',
  Planet = 'planet',
  Star = 'star',
  Comet = 'comet',
  Graphite = 'graphite',
  Wolf = 'wolf',
  Mist = 'mist',
  Pearl = 'pearl',
}

export const SeverityColorValues = {
  [SeverityColorOptions.Ruby]: '#BF3D5E',
  [SeverityColorOptions.Fire]: '#DC4E58',
  [SeverityColorOptions.Curacao]: '#F95F53',
  [SeverityColorOptions.Tiger]: '#F48D38',
  [SeverityColorOptions.Pineapple]: '#FFB94A',
  [SeverityColorOptions.Thunder]: '#FFD255',
  [SeverityColorOptions.Sulfur]: '#FFE480',
  [SeverityColorOptions.Viridian]: '#32B08C',
  [SeverityColorOptions.Rainforest]: '#4ED8A0',
  [SeverityColorOptions.Honeydew]: '#7CE490',
  [SeverityColorOptions.Ocean]: '#4591ED',
  [SeverityColorOptions.Pool]: '#22ADF6',
  [SeverityColorOptions.Laser]: '#00C9FF',
  [SeverityColorOptions.Planet]: '#513CC6',
  [SeverityColorOptions.Star]: '#7A65F2',
  [SeverityColorOptions.Comet]: '#9394FF',
  [SeverityColorOptions.Graphite]: '#545667',
  [SeverityColorOptions.Wolf]: '#8E91A1',
  [SeverityColorOptions.Mist]: '#BEC2CC',
  [SeverityColorOptions.Pearl]: '#E7E8EB',
}

// Log Column Settings
export enum ColumnSettingTypes {
  Visibility = 'visibility',
  Display = 'displayName',
  Label = 'label',
  Color = 'color',
}

export enum ColumnSettingLabelOptions {
  Text = 'text',
  Icon = 'icon',
}

export enum ColumnSettingVisibilityOptions {
  Visible = 'visible',
  Hidden = 'hidden',
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
  Exclude,
  Include,
}

export enum TermPart {
  Exclusion = '-',
  SingleQuoted = "'([^']+)'",
  DoubleQuoted = '"([^"]+)"',
  Attribute = '(\\w+(?=\\:))',
  Colon = '(?::)',
  UnquotedWord = '([\\S]+)',
}

export enum Operator {
  NotLike = '!~',
  Like = '=~',
  Equal = '==',
  NotEqual = '!=',
}

export enum MatchType {
  None = 'no-match',
  Match = 'match',
}

export interface MatchSection {
  id: string
  type: MatchType
  text: string
}

// Table Data
export interface TableData {
  columns: string[]
  values: TimeSeriesValue[][]
}

export type RowHeightHandler = (index: Index) => number

export enum ScrollMode {
  None = 'None',
  TailScrolling = 'TailScrolling',
  TailTop = 'TailTop',
  TimeSelected = 'TimeSelected',
  TimeSelectedScrolling = 'TimeSelectedScroll',
}

// Logs State Getter
export interface State {
  logs: LogsState
}

export type GetState = () => State

export interface TableTime {
  custom?: string
  relative?: number
}
