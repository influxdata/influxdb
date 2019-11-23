// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {
  getFormatter,
  defaultXColumn,
  defaultYColumn,
} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, ScatterViewProperties, TimeZone} from 'src/types'

interface Props {
  children: (config: Config) => JSX.Element
  endTime: number
  fluxGroupKeyUnion?: string[]
  loading: RemoteDataState
  startTime: number
  table: Table
  timeZone: TimeZone
  viewProperties: ScatterViewProperties
}

const ScatterPlot: FunctionComponent<Props> = ({
  children,
  endTime,
  loading,
  startTime,
  timeZone,
  table,
  viewProperties: {
    xAxisLabel,
    yAxisLabel,
    xPrefix,
    xSuffix,
    yPrefix,
    ySuffix,
    fillColumns: storedFill,
    symbolColumns: storedSymbol,
    colors,
    xDomain: storedXDomain,
    yDomain: storedYDomain,
    xColumn: storedXColumn,
    yColumn: storedYColumn,
  },
}) => {
  const fillColumns = storedFill || []
  const symbolColumns = storedSymbol || []

  const xColumn = storedXColumn || defaultXColumn(table)
  const yColumn = storedYColumn || defaultYColumn(table)

  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number')
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    table.getColumn(yColumn, 'number')
  )

  const isValidView =
    xColumn &&
    columnKeys.includes(xColumn) &&
    yColumn &&
    columnKeys.includes(yColumn) &&
    fillColumns.every(col => columnKeys.includes(col)) &&
    symbolColumns.every(col => columnKeys.includes(col))

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length ? colors : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(table.getColumnType(xColumn), {
    prefix: xPrefix,
    suffix: xSuffix,
    timeZone,
  })

  const yFormatter = getFormatter(table.getColumnType(yColumn), {
    prefix: yPrefix,
    suffix: ySuffix,
    timeZone,
  })

  xDomain[0] = Math.min(startTime, xDomain[0])
  xDomain[1] = Math.max(endTime, xDomain[1])

  const config: Config = {
    ...VIS_THEME,
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
        type: 'scatter',
        x: xColumn,
        y: yColumn,
        colors: colorHexes,
        fill: fillColumns,
        symbol: symbolColumns,
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

export default ScatterPlot
