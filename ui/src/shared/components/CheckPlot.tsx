// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Config, Table} from '@influxdata/giraffe'
import {flatMap} from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'
import ThresholdMarkers from 'src/shared/components/ThresholdMarkers'

// Utils
import {getFormatter, filterNoisyColumns} from 'src/shared/utils/vis'
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {
  RemoteDataState,
  CheckViewProperties,
  TimeZone,
  Check,
  Threshold,
} from 'src/types'
import {updateTimeMachineCheck} from 'src/timeMachine/actions'

const X_COLUMN = '_time'
const Y_COLUMN = '_value'

interface DispatchProps {
  updateTimeMachineCheck: typeof updateTimeMachineCheck
}

interface OwnProps {
  table: Table
  check: Partial<Check>
  fluxGroupKeyUnion: string[]
  loading: RemoteDataState
  timeZone: TimeZone
  viewProperties: CheckViewProperties
  children: (config: Config) => JSX.Element
}

type Props = OwnProps & DispatchProps

const CheckPlot: FunctionComponent<Props> = ({
  updateTimeMachineCheck,
  table,
  check,
  fluxGroupKeyUnion,
  loading,
  children,
  timeZone,
  viewProperties: {colors},
}) => {
  let thresholds = []
  if (check && check.type === 'threshold') {
    thresholds = check.thresholds
  }

  const updateTimeMachineCheckThresholds = (thresholds: Threshold[]) => {
    updateTimeMachineCheck({thresholds})
  }

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    null,
    table.getColumn(Y_COLUMN, 'number')
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
        interpolation: 'monotoneX',
        colors,
      },
      {
        type: 'custom',
        render: ({yScale, yDomain}) => (
          <ThresholdMarkers
            key="custom"
            thresholds={thresholds || []}
            onSetThresholds={updateTimeMachineCheckThresholds}
            yScale={yScale}
            yDomain={yDomain}
          />
        ),
      },
    ],
  }

  return (
    <div className="time-series-container time-series-container--alert-check">
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </div>
  )
}

const mdtp: DispatchProps = {
  updateTimeMachineCheck: updateTimeMachineCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(CheckPlot)
