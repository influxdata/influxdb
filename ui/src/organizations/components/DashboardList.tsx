// Libraries
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

// Components
import {IndexList} from 'src/clockface'
import DashboardRow from 'src/organizations/components/DashboardRow'

// APIs
import {cloneDashboard} from 'src/dashboards/apis/v2'

// Constants
import {dashboardCreateFailed} from 'src/shared/copy/notifications'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {Notification} from 'src/types/notifications'
import {Dashboard} from 'src/types/v2'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface DispatchProps {
  notify: (message: Notification) => void
}

interface OwnProps {
  router: InjectedRouter
  orgID: string
  dashboards: Dashboard[]
  emptyState: JSX.Element
  onDeleteDashboard: (dashboard: Dashboard) => void
  onUpdateDashboard: (dashboard: Dashboard) => void
}

type Props = DispatchProps & OwnProps

@ErrorHandling
class DashboardList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="50%" />
          <IndexList.HeaderCell columnName="Modified" width="25%" />
          <IndexList.HeaderCell columnName="" width="25%" />
        </IndexList.Header>
        <IndexList.Body columnCount={2} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private handleCloneDashboard = async (
    dashboard: Dashboard
  ): Promise<void> => {
    const {router, notify, orgID, dashboards} = this.props
    const name = `${dashboard.name} (clone)`
    try {
      const data = await cloneDashboard(
        {
          ...dashboard,
          name,
          orgID,
        },
        dashboards
      )

      router.push(`/dashboards/${data.id}`)
    } catch (error) {
      notify(dashboardCreateFailed())
    }
  }

  private get rows(): JSX.Element[] {
    const {onDeleteDashboard, onUpdateDashboard} = this.props

    return this.props.dashboards.map(d => (
      <DashboardRow
        dashboard={d}
        key={d.id}
        onDeleteDashboard={onDeleteDashboard}
        onUpdateDashboard={onUpdateDashboard}
        onCloneDashboard={this.handleCloneDashboard}
      />
    ))
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect<null, DispatchProps, OwnProps>(
  null,
  mdtp
)(DashboardList)
