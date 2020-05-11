// Libraries
import React, {FunctionComponent, useMemo} from 'react'
import {
  Config,
  Table,
  DomainLabel,
  lineTransform,
  getDomainDataFromLines,
} from '@influxdata/giraffe'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {
  useVisDomainSettings,
  useVisYDomainSettings,
} from 'src/shared/utils/useVisDomainSettings'
import {
  getFormatter,
  geomToInterpolation,
  filterNoisyColumns,
  parseBounds,
  parseYBounds,
  defaultXColumn,
  defaultYColumn,
} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME, VIS_THEME_LIGHT} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {
  RemoteDataState,
  XYViewProperties,
  TimeZone,
  TimeRange,
  Theme,
} from 'src/types'

interface Props {
  children: (config: Config) => JSX.Element
  fluxGroupKeyUnion: string[]
  loading: RemoteDataState
  timeRange: TimeRange | null
  table: Table
  timeZone: TimeZone
  viewProperties: XYViewProperties
  theme: Theme
}

const XYPlot: FunctionComponent<Props> = ({
  children,
  fluxGroupKeyUnion,
  loading,
  timeRange,
  table,
  timeZone,
  viewProperties: {
    geom,
    colors,
    xColumn: storedXColumn,
    yColumn: storedYColumn,
    shadeBelow,
    axes: {
      x: {
        label: xAxisLabel,
        prefix: xTickPrefix,
        suffix: xTickSuffix,
        base: xTickBase,
        bounds: xBounds,
      },
      y: {
        label: yAxisLabel,
        prefix: yTickPrefix,
        suffix: yTickSuffix,
        bounds: yBounds,
        base: yTickBase,
      },
    },
    position,
    timeFormat,
  },
  theme,
}) => {
  const storedXDomain = useMemo(() => parseBounds(xBounds), [xBounds])
  const storedYDomain = useMemo(() => parseYBounds(yBounds), [yBounds])
  const xColumn = storedXColumn || defaultXColumn(table)
  const yColumn = storedYColumn || defaultYColumn(table)

  const columnKeys = table.columnKeys

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

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    table.getColumn(xColumn, 'number'),
    timeRange
  )

  const memoizedYColumnData = useMemo(() => {
    if (position === 'stacked') {
      const {lineData} = lineTransform(
        table,
        xColumn,
        yColumn,
        groupKey,
        colorHexes,
        position
      )
      return getDomainDataFromLines(lineData, DomainLabel.Y)
    }
    return table.getColumn(yColumn, 'number')
  }, [table, yColumn, position])

  const [yDomain, onSetYDomain, onResetYDomain] = useVisYDomainSettings(
    storedYDomain,
    memoizedYColumnData
  )

  const legendColumns = filterNoisyColumns(
    [...groupKey, xColumn, yColumn],
    table
  )

  const xFormatter = getFormatter(table.getColumnType(xColumn), {
    prefix: xTickPrefix,
    suffix: xTickSuffix,
    base: xTickBase,
    timeZone,
    timeFormat,
  })

  const yFormatter = getFormatter(table.getColumnType(yColumn), {
    prefix: yTickPrefix,
    suffix: yTickSuffix,
    base: yTickBase,
    timeZone,
    timeFormat,
  })

  const currentTheme = theme === 'light' ? VIS_THEME_LIGHT : VIS_THEME

  const config: Config = {
    ...currentTheme,
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
    valueFormatters: {
      [xColumn]: xFormatter,
      [yColumn]: yFormatter,
    },
    layers: [
      {
        type: 'line',
        x: xColumn,
        y: yColumn,
        fill: groupKey,
        interpolation,
        position,
        colors: colorHexes,
        shadeBelow: !!shadeBelow,
        shadeBelowOpacity: 0.08,
      },
    ],
  }

  return (
    <>
      {loading === RemoteDataState.Loading && <GraphLoadingDots />}
      {children(config)}
    </>
  )
}

export default XYPlot
