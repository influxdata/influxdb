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
} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  table: Table
  xColumn: string
  yColumn: string
  fillColumns: string[]
}

interface OwnProps {
  children: (props: {
    table: Table
    xColumn: string
    yColumn: string
    fillColumns: string[]
  }) => JSX.Element
}

type Props = StateProps & OwnProps

const VisDataTransform: FunctionComponent<Props> = ({
  table,
  xColumn,
  yColumn,
  fillColumns,
  children,
}) => {
  return children({table, xColumn, yColumn, fillColumns})
}

const mstp = (state: AppState) => {
  const table = getVisTable(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)

  return {
    table,
    xColumn,
    yColumn,
    fillColumns,
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(VisDataTransform)
