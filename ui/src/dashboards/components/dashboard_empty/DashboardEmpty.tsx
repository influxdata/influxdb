// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Button, EmptyState} from '@influxdata/clockface'

// Selectors
import {getOrg} from 'src/organizations/selectors'

// Types
import {IconFont, ComponentSize, ComponentColor} from '@influxdata/clockface'
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface StateProps {
  org: string
  dashboard: string
}

type Props = RouteComponentProps & StateProps

@ErrorHandling
class DashboardEmpty extends Component<Props> {
  public render() {
    return (
      <div className="dashboard-empty">
        <EmptyState size={ComponentSize.Large}>
          <div className="dashboard-empty--graphic">
            <div className="dashbpard-empty--graphic-content" />
          </div>
          <EmptyState.Text>
            This Dashboard doesn't have any <b>Cells</b>, why not add one?
          </EmptyState.Text>
          <Button
            text="Add Cell"
            size={ComponentSize.Medium}
            icon={IconFont.AddCell}
            color={ComponentColor.Primary}
            onClick={this.handleAdd}
            testID="add-cell--button"
          />
        </EmptyState>
      </div>
    )
  }

  private handleAdd = () => {
    const {history, org, dashboard} = this.props
    history.push(`/orgs/${org}/dashboards/${dashboard}/cells/new`)
  }
}

const mstp = (state: AppState): StateProps => {
  return {
    org: getOrg(state).id,
    dashboard: state.currentDashboard.id,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(withRouter(DashboardEmpty))
