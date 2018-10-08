import {LogViewerView, ViewType, ViewShape} from 'src/types/v2/dashboards'

export const NOW = 0
export const DEFAULT_TRUNCATION = true
export const DEFAULT_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

export const LOG_VIEW_NAME = 'LOGS_PAGE'

export const EMPTY_VIEW_PROPERTIES: LogViewerView = {
  columns: [],
  type: ViewType.LogViewer,
  shape: ViewShape.ChronografV2,
}
