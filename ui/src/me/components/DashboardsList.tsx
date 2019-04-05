// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import {EmptyState} from '@influxdata/clockface'

// Types
import {Dashboard, Organization, AppState} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface StateProps {
  dashboards: Dashboard[]
  org: Organization
}

type Props = StateProps

class DashboardList extends PureComponent<Props> {
  public render() {
    const {dashboards, org} = this.props

    if (this.isEmpty) {
      return (
        <EmptyState size={ComponentSize.ExtraSmall}>
          <EmptyState.Text text="You don't have any Dashboards" />
        </EmptyState>
      )
    }

    return (
      <>
        <ul className="link-list">
          {dashboards.map(({id, name}) => (
            <li key={id}>
              <Link to={`/orgs/${org.id}/dashboards/${id}`}>{name}</Link>
            </li>
          ))}
        </ul>
      </>
    )
  }

  private get isEmpty(): boolean {
    return !this.props.dashboards.length
  }
}

const mstp = ({dashboards, orgs: {org}}: AppState): StateProps => ({
  dashboards: dashboards.list,
  org,
})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(DashboardList)
