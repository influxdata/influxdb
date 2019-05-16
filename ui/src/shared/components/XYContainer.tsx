// Libraries
import {get} from 'lodash'
import React, {FunctionComponent, useMemo} from 'react'
import {Config, fluxToTable} from '@influxdata/vis'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {
  getFormatter,
  resolveGeom,
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
import {RemoteDataState, XYView, XYViewGeom} from 'src/types'

interface Props {
  files: string[]
  loading: RemoteDataState
  viewProperties: XYView
  children: (config: Config) => JSX.Element
}

const XYContainer: FunctionComponent<Props> = ({
  files,
  loading,
  children,
  viewProperties: {
    geom,
    colors,
    axes: {
      x: {label: xAxisLabel, bounds: xBounds},
      y: {
        label: yAxisLabel,
        prefix: yTickPrefix,
        suffix: yTickSuffix,
        bounds: yBounds,
      },
    },
  },
}) => {
  const {table, fluxGroupKeyUnion} = useMemo(
    () => fluxToTable(files.join('\n\n')),
    [files]
  )

  // Eventually these will be configurable in the line graph options UI
  const xColumn = chooseXColumn(table)
  const yColumn = chooseYColumn(table)

  const storedXDomain = useMemo(() => parseBounds(xBounds), [xBounds])
  const storedYDomain = useMemo(() => parseBounds(yBounds), [yBounds])

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    get(table, ['columns', xColumn, 'data'], [])
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    get(table, ['columns', yColumn, 'data'], [])
  )

  if (!xColumn || !yColumn) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length
      ? colors.map(c => c.hex)
      : DEFAULT_LINE_COLORS.map(c => c.hex)

  const interpolation =
    resolveGeom(geom) === XYViewGeom.Step ? 'step' : 'linear'

  const groupKey = [...fluxGroupKeyUnion, 'result']

  const legendColumns = filterNoisyColumns(
    [...groupKey, xColumn, yColumn],
    table
  )

  const yFormatter = getFormatter(
    table.columns[yColumn].type,
    yTickPrefix,
    yTickSuffix
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
