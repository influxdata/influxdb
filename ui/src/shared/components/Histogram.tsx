// Libraries
import React, {useMemo, FunctionComponent} from 'react'
import {Plot, Config, Table} from '@influxdata/vis'

// Components
import HistogramTooltip from 'src/shared/components/HistogramTooltip'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Utils
import {useOneWayState} from 'src/shared/utils/useOneWayState'

// Constants
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {HistogramView} from 'src/types/dashboards'

interface Props {
  table: Table
  properties: HistogramView
}

const Histogram: FunctionComponent<Props> = ({
  table,
  properties: {
    xColumn,
    fillColumns,
    binCount,
    position,
    colors,
    xAxisLabel,
    xDomain: defaultXDomain,
  },
}) => {
  const [xDomain, onSetXDomain] = useOneWayState(defaultXDomain)
  const colorHexes = useMemo(() => colors.map(c => c.hex), [colors])

  const isValidView =
    xColumn &&
    table.columns[xColumn] &&
    fillColumns.every(col => !!table.columns[col])

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const config: Config = useMemo(
    () => ({
      table,
      xDomain,
      onSetXDomain,
      xAxisLabel,
      tickFont: 'bold 10px Roboto',
      tickFill: '#8e91a1',
      layers: [
        {
          type: 'histogram' as 'histogram',
          x: xColumn,
          fill: fillColumns,
          binCount,
          position,
          tooltip: HistogramTooltip,
          colors: colorHexes,
        },
      ],
    }),
    [
      table,
      xDomain,
      onSetXDomain,
      xAxisLabel,
      xColumn,
      fillColumns,
      binCount,
      position,
      colorHexes,
    ]
  )

  return <Plot config={config} />
}

export default Histogram
