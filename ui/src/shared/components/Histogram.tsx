// Libraries
import React, {useMemo, useEffect, SFC} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'
import {
  Plot as MinardPlot,
  Histogram as MinardHistogram,
  ColumnType,
  Table,
} from 'src/minard'

// Components
import HistogramTooltip from 'src/shared/components/HistogramTooltip'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Actions
import {tableLoaded} from 'src/timeMachine/actions'

// Utils
import {toMinardTable} from 'src/shared/utils/toMinardTable'
import {useOneWayState} from 'src/shared/utils/useOneWayState'

// Constants
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {FluxTable} from 'src/types'
import {HistogramView} from 'src/types/v2/dashboards'

interface DispatchProps {
  onTableLoaded: typeof tableLoaded
}

interface OwnProps {
  tables: FluxTable[]
  properties: HistogramView
}

type Props = OwnProps & DispatchProps

/*
  Attempt to find valid `x` and `fill` mappings for the histogram.

  The `HistogramView` properties object stores `xColumn` and `fillColumns`
  fields that are used as data-to-aesthetic mappings in the visualization, but
  they may be invalid if the retrieved data for the view has just changed (e.g.
  if a user has submitted a different query). In this case, a `TABLE_LOADED`
  Redux action will eventually emit and the stored fields will be updated
  appropriately, but we still have to be defensive about accessing those fields
  since the component will render before the field resolution takes place.
*/
const resolveMappings = (
  table: Table,
  preferredXColumn: string,
  preferredFillColumns: string[] = []
): [string, string[]] => {
  let x: string = preferredXColumn

  if (!table.columns[x] || table.columnTypes[x] !== ColumnType.Numeric) {
    x = Object.entries(table.columnTypes)
      .filter(([__, type]) => type === ColumnType.Numeric)
      .map(([name]) => name)[0]
  }

  let fill = preferredFillColumns || []

  fill = fill.filter(name => table.columns[name])

  return [x, fill]
}

const Histogram: SFC<Props> = ({
  tables,
  onTableLoaded,
  properties: {
    xColumn,
    fillColumns,
    binCount,
    position,
    colors,
    xDomain: defaultXDomain,
  },
}) => {
  const [xDomain, setXDomain] = useOneWayState(defaultXDomain)
  const colorHexes = useMemo(() => colors.map(c => c.hex), [colors])
  const tableResult = useMemo(() => toMinardTable(tables), [tables])

  useEffect(
    () => {
      onTableLoaded(tableResult)
    },
    [tableResult]
  )

  const {table} = tableResult
  const [x, fill] = resolveMappings(table, xColumn, fillColumns)

  if (!x) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  return (
    <AutoSizer>
      {({width, height}) => (
        <MinardPlot
          table={table}
          width={width}
          height={height}
          xDomain={xDomain}
          onSetXDomain={setXDomain}
        >
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
      )}
    </AutoSizer>
  )
}

const mdtp = {
  onTableLoaded: tableLoaded,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(Histogram)
