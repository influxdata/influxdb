// Libraries
import React, {useMemo, useEffect, SFC} from 'react'
import {connect} from 'react-redux'
import {Plot, Table, Config, isNumeric} from '@influxdata/vis'

// Components
import HistogramTooltip from 'src/shared/components/HistogramTooltip'
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Actions
import {tableLoaded} from 'src/timeMachine/actions'

// Utils
import {toMinardTable} from 'src/shared/utils/toMinardTable'
import {useOneWayState} from 'src/shared/utils/useOneWayState'
import {useSetIdentity} from 'src/shared/utils/useSetIdentity'

// Constants
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {FluxTable} from 'src/types'
import {HistogramView} from 'src/types/dashboards'

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
  preferredXColumnName: string,
  preferredFillColumnNames: string[] = []
): {x: string; fill: string[]} => {
  let x: string = preferredXColumnName

  if (!table.columns[x] || !isNumeric(table.columns[x].type)) {
    x = Object.entries(table.columns)
      .filter(([__, {type}]) => isNumeric(type))
      .map(([name]) => name)[0]
  }

  let fill = preferredFillColumnNames || []

  fill = fill.filter(name => table.columns[name])

  return {x, fill}
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
    xAxisLabel,
    xDomain: defaultXDomain,
  },
}) => {
  const [xDomain, setXDomain] = useOneWayState(defaultXDomain)
  const colorHexes = useMemo(() => colors.map(c => c.hex), [colors])
  const {table} = useMemo(() => toMinardTable(tables), [tables])

  useEffect(() => {
    onTableLoaded(table)
  }, [table])

  const mappings = resolveMappings(table, xColumn, fillColumns)
  const fill = useSetIdentity(mappings.fill)

  if (!mappings.x) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const config: Config = {
    table,
    xDomain,
    onSetXDomain: setXDomain,
    xAxisLabel,
    tickFont: 'bold 10px Roboto',
    tickFill: '#8e91a1',
    layers: [
      {
        type: 'histogram',
        x: mappings.x,
        fill,
        binCount,
        position,
        tooltip: HistogramTooltip,
        colors: colorHexes,
      },
    ],
  }

  return <Plot config={config} />
}

const mdtp = {
  onTableLoaded: tableLoaded,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(Histogram)
