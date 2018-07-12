import _ from 'lodash'
import {
  LogConfig,
  ServerLogConfig,
  ServerColumn,
  LogsTableColumn,
  ServerEncoding,
  SeverityFormat,
  SeverityLevelColor,
} from 'src/types/logs'
import {
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
  EncodingTypes,
  EncodingLabelOptions,
  EncodingVisibilityOptions,
} from 'src/logs/constants'

export const logConfigServerToUI = (
  serverConfig: ServerLogConfig
): LogConfig => {
  const columns = _.get(serverConfig, 'columns', [])
  if (_.isEmpty(columns)) {
    return
  }

  const sortedColumns = sortColumns(columns)

  let severityFormat: SeverityFormatOptions
  let severityLevelColors: SeverityLevelColor[]
  const convertedColumns = sortedColumns.map(c => {
    if (c.name === 'severity') {
      severityFormat = getFormatFromColumn(c)
      severityLevelColors = getLevelColorsFromColumn(c)
    }

    return columnServerToUI(c)
  })

  return {
    tableColumns: convertedColumns,
    severityFormat,
    severityLevelColors,
  }
}

export const sortColumns = (columns: ServerColumn[]): ServerColumn[] => {
  return _.sortBy(columns, c => c.position)
}

export const columnServerToUI = (column: ServerColumn): LogsTableColumn => {
  const internalName = column.name
  const encodings: LogsTableColumn = column.encodings.reduce(
    (acc, e) => {
      if (
        e.type === EncodingTypes.visibility &&
        e.value === EncodingVisibilityOptions.visible
      ) {
        acc.visible = true
      } else if (e.type === EncodingTypes.display) {
        acc.displayName = e.value
      }
      return acc
    },
    {visible: false, displayName: '', internalName}
  )
  return {...encodings, internalName}
}

export const getFormatFromColumn = (
  column: ServerColumn
): SeverityFormatOptions => {
  let hasText = false
  let hasIcon = false

  column.encodings.forEach(e => {
    if (e.type === EncodingTypes.label) {
      if (e.value === EncodingLabelOptions.icon) {
        hasIcon = true
      }
      if (e.value === EncodingLabelOptions.text) {
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

export const getLevelColorsFromColumn = (
  column: ServerColumn
): SeverityLevelColor[] => {
  const colors = column.encodings.filter(e => e.type === EncodingTypes.color)
  return colors.map(c => {
    const level: SeverityLevelOptions = SeverityLevelOptions[c.name]
    const color: SeverityColorOptions = SeverityColorOptions[c.value]
    return {level, color}
  })
}

export const logConfigUIToServer = (config: LogConfig): ServerLogConfig => {
  const tableColumns = _.get(config, 'tableColumns')
  const severityFormat = _.get(config, 'severityFormat')
  const severityLevelColors = _.get(config, 'severityLevelColors')

  if (_.isEmpty(tableColumns)) {
    return {columns: []}
  }

  const columns = tableColumns.map((c, i) => {
    const encodings = getFullEncodings(c, severityFormat, severityLevelColors)
    const name = c.internalName
    const position = i

    return {name, position, encodings}
  })

  return {columns}
}

export const getDisplayAndVisibleEncodings = (
  tableColumn: LogsTableColumn
): ServerEncoding[] => {
  const encodings: ServerEncoding[] = []

  if (tableColumn.visible) {
    encodings.push({
      type: EncodingTypes.visibility,
      value: EncodingVisibilityOptions.visible,
    })
  } else {
    encodings.push({
      type: EncodingTypes.visibility,
      value: EncodingVisibilityOptions.hidden,
    })
  }

  if (!_.isEmpty(tableColumn.displayName)) {
    encodings.push({
      type: EncodingTypes.display,
      value: tableColumn.displayName,
    })
  }

  return encodings
}

export const getLabelEncodings = (format: SeverityFormat): ServerEncoding[] => {
  switch (format) {
    case SeverityFormatOptions.dot:
      return [{type: EncodingTypes.label, value: EncodingLabelOptions.icon}]
    case SeverityFormatOptions.text:
      return [{type: EncodingTypes.label, value: EncodingLabelOptions.text}]
    case SeverityFormatOptions.dotText:
      return [
        {type: EncodingTypes.label, value: EncodingLabelOptions.icon},
        {type: EncodingTypes.label, value: EncodingLabelOptions.text},
      ]
  }
  return null
}

export const getColorEncodings = (
  levelColors: SeverityLevelColor[]
): ServerEncoding[] => {
  return levelColors.map(({color, level}) => {
    return {type: EncodingTypes.color, value: color, name: level}
  })
}

export const getFullEncodings = (
  tableColumn: LogsTableColumn,
  format: SeverityFormat,
  severityLevelColors: SeverityLevelColor[]
): ServerEncoding[] => {
  let encodings = getDisplayAndVisibleEncodings(tableColumn)
  if (tableColumn.internalName === 'severity') {
    encodings = [
      ...encodings,
      ...getLabelEncodings(format),
      ...getColorEncodings(severityLevelColors),
    ]
  }

  return encodings
}
