// Libraries
import {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Table} from '@influxdata/vis'

// Utils
import {
  getVisTable,
  getXColumnSelection,
  getYColumnSelection,
  getFillColumnsSelection,
  getSymbolColumnsSelection,
} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  table: Table
  xColumn: string
  yColumn: string
  fillColumns: string[]
  symbolColumns: string[]
}

interface OwnProps {
  children: (props: {
    table: Table
    xColumn: string
    yColumn: string
    fillColumns: string[]
    symbolColumns: string[]
  }) => JSX.Element
}

type Props = StateProps & OwnProps

const VisDataTransform: FunctionComponent<Props> = ({
  table,
  xColumn,
  yColumn,
  fillColumns,
  symbolColumns,
  children,
}) => {
  return children({table, xColumn, yColumn, fillColumns, symbolColumns})
}

const mstp = (state: AppState): StateProps => {
  const {table} = getVisTable(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)
  const symbolColumns = getSymbolColumnsSelection(state)

  return {
    table,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(VisDataTransform)
