// Libraries
import React, {FunctionComponent, useMemo, useEffect} from 'react'
import {connect} from 'react-redux'
import {Config, fromFlux} from '@influxdata/vis'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Actions
import {setXColumn, setYColumn} from 'src/timeMachine/actions'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {
  getFormatter,
  geomToInterpolation,
  filterNoisyColumns,
  parseBounds,
  chooseXColumn,
  chooseYColumn,
} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, XYView} from 'src/types'

interface DispatchProps {
  onSetXColumn: typeof setXColumn
  onSetYColumn: typeof setYColumn
}

interface OwnProps {
  files: string[]
  loading: RemoteDataState
  viewProperties: XYView
  children: (config: Config) => JSX.Element
}

type Props = OwnProps & DispatchProps

const XYContainer: FunctionComponent<Props> = ({
  files,
  loading,
  children,
  viewProperties: {
    geom,
    colors,
    xColumn,
    yColumn,
    axes: {
      x: {label: xAxisLabel, bounds: xBounds},
      y: {
        label: yAxisLabel,
        prefix: yTickPrefix,
        suffix: yTickSuffix,
        bounds: yBounds,
        base: yTickBase,
      },
    },
  },
  onSetXColumn,
  onSetYColumn,
}) => {
  const {table, fluxGroupKeyUnion} = useMemo(
    () => fromFlux(files.join('\n\n')),
    [files]
  )

  useEffect(() => {
    if (!xColumn) {
      xColumn = chooseXColumn(table)
      onSetXColumn(xColumn)
    }
    if (!yColumn) {
      yColumn = chooseYColumn(table)
      onSetYColumn(yColumn)
    }
  })

  const storedXDomain = useMemo(() => parseBounds(xBounds), [xBounds])
  const storedYDomain = useMemo(() => parseBounds(yBounds), [yBounds])

  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    columnKeys.includes(xColumn) ? table.getColumn(xColumn, 'number') : []
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    columnKeys.includes(yColumn) ? table.getColumn(yColumn, 'number') : []
  )
  if (!xColumn || !yColumn) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length
      ? colors.map(c => c.hex)
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const interpolation = geomToInterpolation(geom)

  const groupKey = [...fluxGroupKeyUnion, 'result']

  const legendColumns = filterNoisyColumns(
    [...groupKey, xColumn, yColumn],
    table
  )

  const yFormatter = getFormatter(
    table.getColumnType(yColumn),
    yTickPrefix,
    yTickSuffix,
    yTickBase
  )

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
    legendColumns,
    valueFormatters: {[yColumn]: yFormatter},
    layers: [
      {
        type: 'line',
        x: xColumn,
        y: yColumn,
        fill: groupKey,
        interpolation,
        colors: colorHexes,
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

const mdtp = {
  onSetXColumn: setXColumn,
  onSetYColumn: setYColumn,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(XYContainer)
