// Libraries
import React, {FunctionComponent, useMemo} from 'react'
import {Config, Table} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

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

interface Props {
  table: Table
  fluxGroupKeyUnion: string[]
  loading: RemoteDataState
  viewProperties: XYView
  children: (config: Config) => JSX.Element
}

const XYContainer: FunctionComponent<Props> = ({
  table,
  fluxGroupKeyUnion,
  loading,
  children,
  viewProperties: {
    geom,
    colors,
    xColumn: storedXColumn,
    yColumn: storedYColumn,
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
}) => {
  const storedXDomain = useMemo(() => parseBounds(xBounds), [xBounds])
  const storedYDomain = useMemo(() => parseBounds(yBounds), [yBounds])

  const xColumn = storedXColumn || chooseXColumn(table)
  const yColumn = storedYColumn || chooseYColumn(table)

  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number')
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    table.getColumn(yColumn, 'number')
  )

  const isValidView =
    xColumn &&
    columnKeys.includes(xColumn) &&
    yColumn &&
    columnKeys.includes(yColumn)

  if (!isValidView) {
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

export default XYContainer
