// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/vis'
import {get} from 'lodash'

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
  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    get(table, ['columns', xColumn, 'data'], [])
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    get(table, ['columns', yColumn, 'data'], [])
  )

  const isValidView =
    xColumn && yColumn && table.columns[xColumn] && table.columns[yColumn]

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colors: string[] =
    storedColors && storedColors.length
      ? storedColors
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const xFormatter = getFormatter(table.columns[xColumn].type, xPrefix, xSuffix)
  const yFormatter = getFormatter(table.columns[yColumn].type, yPrefix, ySuffix)

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
