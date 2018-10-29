// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Actions
import {addCellAsync} from 'src/dashboards/actions/v2'

// Types
import {Dashboard} from 'src/types/v2/dashboards'
import {GRAPH_TYPES} from 'src/dashboards/graphics/graph'

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
          This Dashboard doesn't have any <strong>Cells</strong>,<br />
          why not add one?
        </p>
        <div className="dashboard-empty--menu">
          {GRAPH_TYPES.map(graphType => (
            <div key={graphType.type} className="dashboard-empty--menu-option">
              <div onClick={this.handleAddCell(graphType.type)}>
                {graphType.graphic}
                <p>{graphType.menuOption}</p>
              </div>
            </div>
          ))}
        </div>
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
