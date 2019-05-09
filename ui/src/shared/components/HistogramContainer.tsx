// Libraries
import React, {FunctionComponent} from 'react'
import {Config, Table} from '@influxdata/vis'
import {get} from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {formatNumber} from 'src/shared/utils/vis'

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
  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    get(table, ['columns', xColumn, 'data'], [])
  )

  const isValidView =
    xColumn &&
    table.columns[xColumn] &&
    fillColumns.every(col => !!table.columns[col])

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length
      ? colors.map(c => c.hex)
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const config: Config = {
    ...VIS_THEME,
    table,
    xAxisLabel,
    xTickFormatter: formatNumber,
    xDomain,
    onSetXDomain,
    onResetXDomain,
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
    <div className="histogram-container">
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </div>
  )
}

export default HistogramContainer
