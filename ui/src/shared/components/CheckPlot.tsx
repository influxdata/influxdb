// Libraries
import React, {useState, FunctionComponent} from 'react'
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
  Threshold,
} from 'src/types'

const X_COLUMN = '_time'
const Y_COLUMN = '_value'

const THRESHOLDS: Threshold[] = [
  {
    level: 'UNKNOWN',
    lowerBound: 20,
    allValues: false,
  },
]

interface Props {
  table: Table
  fluxGroupKeyUnion: string[]
  loading: RemoteDataState
  timeZone: TimeZone
  viewProperties: CheckViewProperties
  children: (config: Config) => JSX.Element
}

const CheckPlot: FunctionComponent<Props> = ({
  table,
  fluxGroupKeyUnion,
  loading,
  children,
  timeZone,
}) => {
  const [thresholds, setThresholds] = useState(THRESHOLDS)

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    [0, 100],
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

  const yTicks = flatMap(thresholds, (t: any) => [
    t.value,
    t.minValue,
    t.maxValue,
  ]).filter(t => t !== undefined)

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
      },
      {
        type: 'custom',
        render: ({yScale, yDomain}) => (
          <ThresholdMarkers
            key="custom"
            thresholds={thresholds}
            onSetThresholds={setThresholds}
            yScale={yScale}
            yDomain={yDomain}
          />
        ),
      },
    ],
  }

  return (
    <div className="vis-plot-container vis-plot-container--alert-check">
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </div>
  )
}

export default CheckPlot
