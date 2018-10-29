import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'

import {
  LogConfig,
  LogsTableColumn,
  SeverityFormat,
  SeverityLevelColor,
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
  ColumnSettingTypes,
  ColumnSettingLabelOptions,
  ColumnSettingVisibilityOptions,
} from 'src/types/logs'

import {
  View,
  LogViewerView,
  LogViewerColumn,
  LogViewerColumnSetting,
} from 'src/types/v2/dashboards'

import {
  DEFAULT_TRUNCATION,
  LOG_VIEW_NAME,
  EMPTY_VIEW_PROPERTIES,
} from 'src/logs/constants'

export const serverToUIConfig = (logView: View): LogConfig => {
  const logConfigID = getDeep<string>(logView, 'id', null)
  const logsConfigLink = getDeep<string>(logView, 'links.self', null)
  const logViewProperties = getDeep<LogViewerView>(logView, 'properties', null)
  const columns = getDeep<LogViewerColumn[]>(logViewProperties, 'columns', [])

  if (_.isEmpty(columns)) {
    return
  }

  const sortedColumns = sortViewColumns(columns)

  let severityFormat: SeverityFormatOptions
  let severityLevelColors: SeverityLevelColor[]
  const convertedColumns = sortedColumns.map(c => {
    if (c.name === 'severity') {
      severityFormat = generateColumnFormatConfig(c)
      severityLevelColors = generateColumnColorsConfig(c)
    }

    return generateColumnConfig(c)
  })

  return {
    id: logConfigID,
    link: logsConfigLink,
    isTruncated: DEFAULT_TRUNCATION,
    tableColumns: convertedColumns,
    severityFormat,
    severityLevelColors,
  }
}

export const sortViewColumns = (
  columns: LogViewerColumn[]
): LogViewerColumn[] => {
  return _.sortBy(columns, c => c.position)
}

export const generateColumnConfig = (
  column: LogViewerColumn
): LogsTableColumn => {
  const internalName = column.name
  const settings: LogsTableColumn = column.settings.reduce(
    (acc, e) => {
      if (
        e.type === ColumnSettingTypes.Visibility &&
        e.value === ColumnSettingVisibilityOptions.Visible
      ) {
        acc.visible = true
      } else if (e.type === ColumnSettingTypes.Display) {
        acc.displayName = e.value
      }
      return acc
    },
    {visible: false, displayName: '', internalName}
  )
  return {...settings, internalName}
}

export const generateColumnFormatConfig = (
  column: LogViewerColumn
): SeverityFormatOptions => {
  let hasText = false
  let hasIcon = false

  column.settings.forEach(e => {
    if (e.type === ColumnSettingTypes.Label) {
      if (e.value === ColumnSettingLabelOptions.Icon) {
        hasIcon = true
      }
      if (e.value === ColumnSettingLabelOptions.Text) {
        hasText = true
      }
    }
  })

  if (hasText && hasIcon) {
    return SeverityFormatOptions.DotText
  } else if (hasText) {
    return SeverityFormatOptions.Text
  } else {
    return SeverityFormatOptions.Dot
  }
}

export const generateColumnColorsConfig = (
  column: LogViewerColumn
): SeverityLevelColor[] => {
  const colors = column.settings.filter(
    e => e.type === ColumnSettingTypes.Color
  )
  return colors.map(c => {
    const level: SeverityLevelOptions = SeverityLevelOptions[capitalize(c.name)]
    const color: SeverityColorOptions =
      SeverityColorOptions[capitalize(c.value)]
    return {level, color}
  })
}

export const capitalize = (word: string): string =>
  word.charAt(0).toUpperCase() + word.slice(1)

export const uiToServerConfig = (config: LogConfig): View => {
  const properties: LogViewerView = generateViewProperties(config)

  return {
    properties,
    id: config.id,
    name: LOG_VIEW_NAME,
  }
}

export const generateViewProperties = (config: LogConfig): LogViewerView => {
  const tableColumns = getDeep<LogsTableColumn[]>(config, 'tableColumns', [])
  const severityFormat = getDeep<SeverityFormat>(config, 'severityFormat', null)
  const severityLevelColors = getDeep<SeverityLevelColor[]>(
    config,
    'severityLevelColors',
    []
  )

  if (_.isEmpty(tableColumns)) {
    return EMPTY_VIEW_PROPERTIES
  }

  const columns: LogViewerColumn[] = tableColumns.map((c, i) => {
    const settings = generateViewColumnSettings(
      c,
      severityFormat,
      severityLevelColors
    )
    const name = c.internalName
    const position = i

    return {name, position, settings}
  })

  return {
    ...EMPTY_VIEW_PROPERTIES,
    columns,
  }
}

export const generateViewColumns = (
  tableColumn: LogsTableColumn
): LogViewerColumnSetting[] => {
  const settings: LogViewerColumnSetting[] = []

  if (tableColumn.visible) {
    settings.push({
      type: ColumnSettingTypes.Visibility,
      value: ColumnSettingVisibilityOptions.Visible,
    })
  } else {
    settings.push({
      type: ColumnSettingTypes.Visibility,
      value: ColumnSettingVisibilityOptions.Hidden,
    })
  }

  if (!_.isEmpty(tableColumn.displayName)) {
    settings.push({
      type: ColumnSettingTypes.Display,
      value: tableColumn.displayName,
    })
  }

  return settings
}

export const generateViewColumnSettings = (
  tableColumn: LogsTableColumn,
  format: SeverityFormat,
  severityLevelColors: SeverityLevelColor[]
): LogViewerColumnSetting[] => {
  let settings = generateViewColumns(tableColumn)
  if (tableColumn.internalName === 'severity') {
    settings = [
      ...settings,
      ...generateViewColumnSeverityLabels(format),
      ...generateViewColumnSeverityColors(severityLevelColors),
    ]
  }

  return settings
}

export const generateViewColumnSeverityLabels = (
  format: SeverityFormat
): LogViewerColumnSetting[] => {
  switch (format) {
    case SeverityFormatOptions.Dot:
      return [
        {type: ColumnSettingTypes.Label, value: ColumnSettingLabelOptions.Icon},
      ]
    case SeverityFormatOptions.Text:
      return [
        {type: ColumnSettingTypes.Label, value: ColumnSettingLabelOptions.Text},
      ]
    case SeverityFormatOptions.DotText:
      return [
        {type: ColumnSettingTypes.Label, value: ColumnSettingLabelOptions.Icon},
        {type: ColumnSettingTypes.Label, value: ColumnSettingLabelOptions.Text},
      ]
  }
  return null
}

export const generateViewColumnSeverityColors = (
  levelColors: SeverityLevelColor[]
): LogViewerColumnSetting[] => {
  return levelColors.map(({color, level}) => {
    return {type: ColumnSettingTypes.Color, value: color, name: level}
  })
}
