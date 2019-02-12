// Libraries
import React, {useMemo, SFC} from 'react'
import {connect} from 'react-redux'
import {Plot as MinardPlot, Histogram as MinardHistogram} from 'src/minard'

// Components
import HistogramTooltip from 'src/shared/components/HistogramTooltip'

// Utils
import {toMinardTable} from 'src/shared/utils/toMinardTable'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {FluxTable} from 'src/types'
import {AppState} from 'src/types/v2'
import {HistogramView} from 'src/types/v2/dashboards'

interface StateProps {
  properties: HistogramView
}

interface OwnProps {
  width: number
  height: number
  tables: FluxTable[]
}

type Props = OwnProps & StateProps

const Histogram: SFC<Props> = props => {
  const {tables, width, height} = props
  const {x, fill, binCount, position, colors} = props.properties
  const {table} = useMemo(() => toMinardTable(tables), [tables])
  const colorHexes = colors.map(c => c.hex)

  return (
    <MinardPlot table={table} width={width} height={height} colors={colorHexes}>
      {env => (
        <MinardHistogram
          env={env}
          x={x}
          fill={fill}
          bins={binCount}
          position={position}
          tooltip={HistogramTooltip}
        />
      )}
    </MinardPlot>
  )
}

const mstp = (state: AppState) => {
  const properties = getActiveTimeMachine(state).view
    .properties as HistogramView

  return {properties}
}

export default connect<StateProps, {}, OwnProps>(mstp)(Histogram)
