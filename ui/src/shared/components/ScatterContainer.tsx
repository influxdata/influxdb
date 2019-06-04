// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/vis'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {getFormatter, chooseXColumn, chooseYColumn} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, ScatterView} from 'src/types'

interface Props {
  table: Table
  fluxGroupKeyUnion?: string[]
  loading: RemoteDataState
  viewProperties: ScatterView
  children: (config: Config) => JSX.Element
}

const ScatterContainer: FunctionComponent<Props> = ({
  table,
  loading,
  children,
  viewProperties: {
    xAxisLabel,
    yAxisLabel,
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

  const xColumn = storedXColumn || chooseXColumn(table)
  const yColumn = storedYColumn || chooseYColumn(table)

  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    columnKeys.includes(xColumn) ? table.getColumn(xColumn, 'number') : []
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    columnKeys.includes(yColumn) ? table.getColumn(yColumn, 'number') : []
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

  const yFormatter = getFormatter(
    table.getColumnType(yColumn),
    yPrefix,
    ySuffix
  )

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
    <div className="vis-plot-container">
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </div>
  )
}

export default ScatterContainer
