// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {getFormatter} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, HistogramView} from 'src/types'

interface Props {
  table: Table
  loading: RemoteDataState
  viewProperties: HistogramView
  children: (config: Config) => JSX.Element
}

const HistogramContainer: FunctionComponent<Props> = ({
  table,
  loading,
  children,
  viewProperties: {
    xColumn,
    fillColumns,
    binCount,
    position,
    colors,
    xAxisLabel,
    xDomain: storedXDomain,
  },
}) => {
  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number')
  )

  const isValidView =
    xColumn &&
    columnKeys.includes(xColumn) &&
    fillColumns.every(col => columnKeys.includes(col))

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length
      ? colors.map(c => c.hex)
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(table.getColumnType(xColumn))

  const config: Config = {
    ...VIS_THEME,
    table,
    xAxisLabel,
    xDomain,
    onSetXDomain,
    onResetXDomain,
    valueFormatters: {[xColumn]: xFormatter},
    layers: [
      {
        type: 'histogram',
        x: xColumn,
        colors: colorHexes,
        fill: fillColumns,
        binCount,
        position,
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

export default HistogramContainer
