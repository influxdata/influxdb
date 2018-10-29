import {LogViewerView, ViewType, ViewShape} from 'src/types/v2/dashboards'
import {
  SeverityColorValues,
  SeverityColorOptions,
  SeverityLevelOptions,
  TableData,
} from 'src/types/logs'

export const NOW = 0
export const DEFAULT_TRUNCATION = true
export const DEFAULT_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

export const LOG_VIEW_NAME = 'LOGS_PAGE'

export const EMPTY_VIEW_PROPERTIES: LogViewerView = {
  columns: [],
  type: ViewType.LogViewer,
  shape: ViewShape.ChronografV2,
}

export const DEFAULT_SEVERITY_LEVELS = {
  [SeverityLevelOptions.Emerg]: SeverityColorOptions.Ruby,
  [SeverityLevelOptions.Alert]: SeverityColorOptions.Fire,
  [SeverityLevelOptions.Crit]: SeverityColorOptions.Curacao,
  [SeverityLevelOptions.Err]: SeverityColorOptions.Tiger,
  [SeverityLevelOptions.Warning]: SeverityColorOptions.Pineapple,
  [SeverityLevelOptions.Notice]: SeverityColorOptions.Rainforest,
  [SeverityLevelOptions.Info]: SeverityColorOptions.Star,
  [SeverityLevelOptions.Debug]: SeverityColorOptions.Wolf,
}

export const SEVERITY_COLORS = [
  {
    hex: SeverityColorValues[SeverityColorOptions.Ruby],
    name: SeverityColorOptions.Ruby,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Fire],
    name: SeverityColorOptions.Fire,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Curacao],
    name: SeverityColorOptions.Curacao,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Tiger],
    name: SeverityColorOptions.Tiger,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Pineapple],
    name: SeverityColorOptions.Pineapple,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Thunder],
    name: SeverityColorOptions.Thunder,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Sulfur],
    name: SeverityColorOptions.Sulfur,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Viridian],
    name: SeverityColorOptions.Viridian,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Rainforest],
    name: SeverityColorOptions.Rainforest,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Honeydew],
    name: SeverityColorOptions.Honeydew,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Ocean],
    name: SeverityColorOptions.Ocean,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Pool],
    name: SeverityColorOptions.Pool,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Laser],
    name: SeverityColorOptions.Laser,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Planet],
    name: SeverityColorOptions.Planet,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Star],
    name: SeverityColorOptions.Star,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Comet],
    name: SeverityColorOptions.Comet,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Graphite],
    name: SeverityColorOptions.Graphite,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Wolf],
    name: SeverityColorOptions.Wolf,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Mist],
    name: SeverityColorOptions.Mist,
  },
  {
    hex: SeverityColorValues[SeverityColorOptions.Pearl],
    name: SeverityColorOptions.Pearl,
  },
]

export const DEFAULT_TAIL_CHUNK_DURATION_MS = 5000
export const DEFAULT_MAX_TAIL_BUFFER_DURATION_MS = 30000

export const defaultTableData: TableData = {
  columns: [
    'time',
    'severity',
    'timestamp',
    'message',
    'facility',
    'procid',
    'appname',
    'hostname',
  ],
  values: [],
}
