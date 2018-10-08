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
        e.type === ColumnSettingTypes.visibility &&
        e.value === ColumnSettingVisibilityOptions.visible
      ) {
        acc.visible = true
      } else if (e.type === ColumnSettingTypes.display) {
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
    if (e.type === ColumnSettingTypes.label) {
      if (e.value === ColumnSettingLabelOptions.icon) {
        hasIcon = true
      }
      if (e.value === ColumnSettingLabelOptions.text) {
        hasText = true
      }
    }
  })

  if (hasText && hasIcon) {
    return SeverityFormatOptions.dotText
  } else if (hasText) {
    return SeverityFormatOptions.text
  } else {
    return SeverityFormatOptions.dot
  }
}

export const generateColumnColorsConfig = (
  column: LogViewerColumn
): SeverityLevelColor[] => {
  const colors = column.settings.filter(
    e => e.type === ColumnSettingTypes.color
  )
  return colors.map(c => {
    const level: SeverityLevelOptions = SeverityLevelOptions[c.name]
    const color: SeverityColorOptions = SeverityColorOptions[c.value]
    return {level, color}
  })
}

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
      type: ColumnSettingTypes.visibility,
      value: ColumnSettingVisibilityOptions.visible,
    })
  } else {
    settings.push({
      type: ColumnSettingTypes.visibility,
      value: ColumnSettingVisibilityOptions.hidden,
    })
  }

  if (!_.isEmpty(tableColumn.displayName)) {
    settings.push({
      type: ColumnSettingTypes.display,
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
    case SeverityFormatOptions.dot:
      return [
        {type: ColumnSettingTypes.label, value: ColumnSettingLabelOptions.icon},
      ]
    case SeverityFormatOptions.text:
      return [
        {type: ColumnSettingTypes.label, value: ColumnSettingLabelOptions.text},
      ]
    case SeverityFormatOptions.dotText:
      return [
        {type: ColumnSettingTypes.label, value: ColumnSettingLabelOptions.icon},
        {type: ColumnSettingTypes.label, value: ColumnSettingLabelOptions.text},
      ]
  }
  return null
}

export const generateViewColumnSeverityColors = (
  levelColors: SeverityLevelColor[]
): LogViewerColumnSetting[] => {
  return levelColors.map(({color, level}) => {
    return {type: ColumnSettingTypes.color, value: color, name: level}
  })
}
