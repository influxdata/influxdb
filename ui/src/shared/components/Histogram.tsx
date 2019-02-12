// Libraries
import React, {useMemo, useEffect, SFC} from 'react'
import {connect} from 'react-redux'
import {
  Plot as MinardPlot,
  Histogram as MinardHistogram,
  ColumnType,
} from 'src/minard'

// Components
import HistogramTooltip from 'src/shared/components/HistogramTooltip'

// Actions
import {tableLoaded} from 'src/timeMachine/actions'

// Utils
import {toMinardTable} from 'src/shared/utils/toMinardTable'

// Types
import {FluxTable} from 'src/types'
import {HistogramView} from 'src/types/v2/dashboards'

interface DispatchProps {
  onTableLoaded: typeof tableLoaded
}

interface OwnProps {
  width: number
  height: number
  tables: FluxTable[]
  properties: HistogramView
}

type Props = OwnProps & DispatchProps

const Histogram: SFC<Props> = props => {
  const {tables, width, height, onTableLoaded} = props
  const {xColumn, fillColumns, binCount, position, colors} = props.properties
  const colorHexes = colors.map(c => c.hex)

  const toMinardTableResult = useMemo(() => toMinardTable(tables), [tables])

  useEffect(
    () => {
      onTableLoaded(toMinardTableResult)
    },
    [toMinardTableResult]
  )

  const {table} = toMinardTableResult

  // The view properties object stores `xColumn` and `fillColumns` fields that
  // are used as parameters for the visualization, but they may be invalid if
  // the retrieved data for the view has just changed (e.g. if a user has
  // changed their query). In this case, the `TABLE_LOADED` action will emit
  // from the above effect and the stored fields will be updated appropriately,
  // but we still have to be defensive about accessing those fields since the
  // component will render before the field resolution takes place.
  let x: string
  let fill: string[]

  if (table.columns[xColumn]) {
    x = xColumn
  } else {
    x = Object.entries(table.columnTypes)
      .filter(([__, type]) => type === ColumnType.Numeric)
      .map(([name]) => name)[0]
  }

  if (fillColumns) {
    fill = fillColumns.filter(name => table.columns[name])
  } else {
    fill = []
  }

  return (
    <MinardPlot table={table} width={width} height={height}>
      {env => (
        <MinardHistogram
          env={env}
          x={x}
          fill={fill}
          binCount={binCount}
          position={position}
          tooltip={HistogramTooltip}
          colors={colorHexes}
        />
      )}
    </MinardPlot>
  )
}

const mdtp = {
  onTableLoaded: tableLoaded,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(Histogram)
