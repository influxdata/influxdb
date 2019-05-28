// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/vis'

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
import {RemoteDataState, HeatmapView} from 'src/types'

interface Props {
  table: Table
  loading: RemoteDataState
  viewProperties: HeatmapView
  children: (config: Config) => JSX.Element
}

const HeatmapContainer: FunctionComponent<Props> = ({
  table,
  loading,
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
  },
  children,
}) => {
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
    yColumn &&
    xColumn in table.columnKeys &&
    yColumn in table.columnKeys

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colors: string[] =
    storedColors && storedColors.length
      ? storedColors
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(
    table.getColumnType(xColumn),
    xPrefix,
    xSuffix
  )

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
    <div className="vis-plot-container">
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </div>
  )
}

export default HeatmapContainer
