// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Config, Table} from '@influxdata/giraffe'
import {flatMap} from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import ThresholdMarkers from 'src/shared/components/ThresholdMarkers'
import EventMarkers from 'src/shared/components/EventMarkers'

// Utils
import {getFormatter, filterNoisyColumns} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'

// Types
import {
  CheckViewProperties,
  TimeZone,
  CheckType,
  StatusRow,
  Threshold,
} from 'src/types'
import {useCheckYDomain} from 'src/alerting/utils/vis'
import {updateThresholds} from 'src/alerting/actions/alertBuilder'

const X_COLUMN = '_time'
const Y_COLUMN = '_value'

interface DispatchProps {
  onUpdateThresholds: typeof updateThresholds
}

interface OwnProps {
  table: Table
  checkType: CheckType
  thresholds: Threshold[]
  fluxGroupKeyUnion: string[]
  timeZone: TimeZone
  viewProperties: CheckViewProperties
  children: (config: Config) => JSX.Element
  statuses: StatusRow[][]
}

type Props = OwnProps & DispatchProps

const CheckPlot: FunctionComponent<Props> = ({
  table,
  fluxGroupKeyUnion,
  children,
  timeZone,
  statuses,
  checkType,
  thresholds,
  onUpdateThresholds,
  viewProperties: {colors},
}) => {
  const [yDomain, onSetYDomain, onResetYDomain] = useCheckYDomain(
    table.getColumn(Y_COLUMN, 'number'),
    thresholds
  )

  const columnKeys = table.columnKeys
  const isValidView =
    columnKeys.includes(X_COLUMN) && columnKeys.includes(Y_COLUMN)

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const groupKey = [...fluxGroupKeyUnion, 'result']

  const xFormatter = getFormatter(table.getColumnType(X_COLUMN), {
    timeZone,
    trimZeros: false,
  })

  const yFormatter = getFormatter(table.getColumnType(Y_COLUMN), {
    timeZone,
    trimZeros: false,
  })

  const legendColumns = filterNoisyColumns(
    [...groupKey, X_COLUMN, Y_COLUMN],
    table
  )

  const thresholdValues = flatMap(thresholds, (t: any) => [
    t.value,
    t.minValue,
    t.maxValue,
  ]).filter(t => t !== undefined)

  const yTicks = thresholdValues.length ? thresholdValues : null

  const colorHexes =
    colors && colors.length
      ? colors.map(c => c.hex)
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const config: Config = {
    ...VIS_THEME,
    table,
    legendColumns,
    yTicks,
    yDomain,
    onSetYDomain,
    onResetYDomain,
    valueFormatters: {
      [X_COLUMN]: xFormatter,
      [Y_COLUMN]: yFormatter,
    },
    layers: [
      {
        type: 'line',
        x: X_COLUMN,
        y: Y_COLUMN,
        fill: groupKey,
        interpolation: 'linear',
        colors: colorHexes,
      },
      {
        type: 'custom',
        render: ({yScale, yDomain}) => (
          <ThresholdMarkers
            key="thresholds"
            thresholds={checkType === 'threshold' ? thresholds : []}
            onSetThresholds={onUpdateThresholds}
            yScale={yScale}
            yDomain={yDomain}
          />
        ),
      },
      {
        type: 'custom',
        render: ({xScale, xDomain}) => (
          <EventMarkers
            key="events"
            eventsArray={statuses}
            xScale={xScale}
            xDomain={xDomain}
            xFormatter={xFormatter}
          />
        ),
      },
    ],
  }

  return (
    <div className="time-series-container time-series-container--alert-check">
      {children(config)}
    </div>
  )
}

const mdtp: DispatchProps = {
  onUpdateThresholds: updateThresholds,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(CheckPlot)
