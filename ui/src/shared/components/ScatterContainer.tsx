// Libraries
import React, {FunctionComponent, useEffect} from 'react'
import {connect} from 'react-redux'
import {Config, Table} from '@influxdata/vis'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'
import GraphLoadingDots from 'src/shared/components/GraphLoadingDots'

// Utils
import {useVisDomainSettings} from 'src/shared/utils/useVisDomainSettings'
import {getFormatter, chooseYColumn, chooseXColumn} from 'src/shared/utils/vis'

// Constants
import {VIS_THEME} from 'src/shared/constants'
import {DEFAULT_LINE_COLORS} from 'src/shared/constants/graphColorPalettes'
import {INVALID_DATA_COPY} from 'src/shared/copy/cell'

// Types
import {RemoteDataState, ScatterView} from 'src/types'
import {setFillColumns, setSymbolColumns} from 'src/timeMachine/actions'

interface OwnProps {
  table: Table
  fluxGroupKeyUnion?: string[]
  loading: RemoteDataState
  viewProperties: ScatterView
  children: (config: Config) => JSX.Element
}

interface DispatchProps {
  onSetFillColumns: typeof setFillColumns
  onSetSymbolColumns: typeof setSymbolColumns
}

type Props = OwnProps & DispatchProps

const ScatterContainer: FunctionComponent<Props> = ({
  table,
  loading,
  children,
  fluxGroupKeyUnion,
  viewProperties: {
    xAxisLabel,
    yAxisLabel,
    yPrefix,
    ySuffix,
    fillColumns: storedFill,
    symbolColumns: storedSymbol,
    colors,
    xDomain: storedXDomain,
    yDomain: storedYDomain,
  },
  onSetFillColumns,
  onSetSymbolColumns,
}) => {
  useEffect(() => {
    if (fluxGroupKeyUnion && (!storedSymbol || !storedFill)) {
      // if new view, maximize variations in symbol and color
      const filteredGroupKeys = fluxGroupKeyUnion.filter(
        k =>
          ![
            'result',
            'table',
            '_measurement',
            '_start',
            '_stop',
            '_field',
          ].includes(k)
      )
      if (!storedSymbol) {
        onSetSymbolColumns(filteredGroupKeys)
      }
      if (!storedFill) {
        onSetFillColumns(filteredGroupKeys)
      }
    }
  })

  const fillColumns = storedFill || []
  const symbolColumns = storedSymbol || []

  // TODO: allow xcolumn and ycolumn to be user selectable
  const xColumn = chooseXColumn(table)
  const yColumn = chooseYColumn(table)

  const columnKeys = table.columnKeys

  const [xDomain, onSetXDomain, onResetXDomain] = useVisDomainSettings(
    storedXDomain,
    columnKeys.includes(xColumn) ? table.getColumn(xColumn, 'number') : []
  )

  const [yDomain, onSetYDomain, onResetYDomain] = useVisDomainSettings(
    storedYDomain,
    columnKeys.includes(yColumn) ? table.getColumn(yColumn, 'number') : []
  )

  const isValidView =
    xColumn &&
    columnKeys.includes(xColumn) &&
    yColumn &&
    columnKeys.includes(yColumn) &&
    fillColumns.every(col => columnKeys.includes(col)) &&
    symbolColumns.every(col => columnKeys.includes(col))

  if (!isValidView) {
    return <EmptyGraphMessage message={INVALID_DATA_COPY} />
  }

  const colorHexes =
    colors && colors.length ? colors : DEFAULT_LINE_COLORS.map(c => c.hex)

  const yFormatter = getFormatter(
    table.getColumnType(yColumn),
    yPrefix,
    ySuffix
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
    valueFormatters: {
      [yColumn]: yFormatter,
    },
    layers: [
      {
        type: 'scatter',
        x: xColumn,
        y: yColumn,
        colors: colorHexes,
        fill: fillColumns,
        symbol: symbolColumns,
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
  onSetFillColumns: setFillColumns,
  onSetSymbolColumns: setSymbolColumns,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(ScatterContainer)
