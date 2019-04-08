// Libraries
import {FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Table} from '@influxdata/vis'

// Utils
import {
  getVisTable,
  getXColumnSelection,
  getFillColumnsSelection,
} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'

interface StateProps {
  table: Table
  xColumn: string
  fillColumns: string[]
}

interface OwnProps {
  children: (props: {
    table: Table
    xColumn: string
    fillColumns: string[]
  }) => JSX.Element
}

type Props = StateProps & OwnProps

const HistogramTransform: FunctionComponent<Props> = ({
  table,
  xColumn,
  fillColumns,
  children,
}) => {
  return children({table, xColumn, fillColumns})
}

const mstp = (state: AppState) => {
  const table = getVisTable(state)
  const xColumn = getXColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)

  return {
    table,
    xColumn,
    fillColumns,
  }
}

const mdtp = {}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  mdtp
)(HistogramTransform)
