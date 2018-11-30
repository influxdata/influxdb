// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Actions
import {addCellAsync} from 'src/dashboards/actions/v2'

// Types
import {Dashboard} from 'src/api'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  dashboard: Dashboard
}

interface Actions {
  addDashboardCell: typeof addCellAsync
}

@ErrorHandling
class DashboardEmpty extends Component<Props & Actions> {
  constructor(props) {
    super(props)
  }

  public handleAddCell = __ => async () => {
    const {dashboard, addDashboardCell} = this.props
    await addDashboardCell(dashboard)
  }

  public render() {
    return (
      <div className="dashboard-empty">
        <p>
          This Dashboard doesn't have any <strong>Cells</strong>, why not add
          one?
        </p>
      </div>
    )
  }
}

const mdtp = {
  addDashboardCell: addCellAsync,
}

export default connect(
  null,
  mdtp
)(DashboardEmpty)
