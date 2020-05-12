// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {
  useVisXDomainSettings,
  useVisYDomainSettings,
} from 'src/shared/utils/useVisDomainSettings'
import {getFormatter} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME, VIS_THEME_LIGHT} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {
  RemoteDataState,
  HeatmapViewProperties,
  TimeZone,
  TimeRange,
  Theme,
} from 'src/types'

interface Props {
  loading: RemoteDataState
  timeRange: TimeRange | null
  table: Table
  timeZone: TimeZone
  viewProperties: HeatmapViewProperties
  children: (config: Config) => JSX.Element
  theme?: Theme
}

const HeatmapPlot: FunctionComponent<Props> = ({
  loading,
  timeRange,
  table,
  timeZone,
  viewProperties: {
    xColumn,
    yColumn,
    xDomain: storedXDomain,
    yDomain: storedYDomain,
    xAxisLabel,
    yAxisLabel,
    xPrefix,
    xSuffix,
    yPrefix,
    ySuffix,
    colors: storedColors,
    binSize,
    timeFormat,
  },
  children,
  theme,
}) => {
  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisXDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number'),
    timeRange
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisYDomainSettings(
    storedYDomain,
    table.getColumn(yColumn, 'number')
  )

  const isValidView =
    xColumn &&
    yColumn &&
    columnKeys.includes(yColumn) &&
    columnKeys.includes(xColumn)

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colors: string[] =
    storedColors && storedColors.length
      ? storedColors
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(table.getColumnType(xColumn), {
    prefix: xPrefix,
    suffix: xSuffix,
    timeZone,
    timeFormat,
  })

  const yFormatter = getFormatter(table.getColumnType(yColumn), {
    prefix: yPrefix,
    suffix: ySuffix,
    timeZone,
    timeFormat,
  })

  const currentTheme = theme === 'light' ? VIS_THEME_LIGHT : VIS_THEME

  const config: Config = {
    ...currentTheme,
    table,
    xAxisLabel,
    yAxisLabel,
    xDomain,
    onSetXDomain,
    onResetXDomain,
    yDomain,
    onSetYDomain,
    onResetYDomain,
    valueFormatters: {
      [xColumn]: xFormatter,
      [yColumn]: yFormatter,
    },
    layers: [
      {
        type: 'heatmap',
        x: xColumn,
        y: yColumn,
        colors,
        binSize,
      },
    ],
  }

  return (
    <>
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </>
  )
}

export default HeatmapPlot
