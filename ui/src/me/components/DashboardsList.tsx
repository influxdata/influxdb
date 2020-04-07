// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'
import {sortBy} from 'lodash'

// Components
import {EmptyState, DapperScrollbars} from '@influxdata/clockface'

// Types
import {Dashboard, Organization, AppState, ResourceType} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'
import {getSortedDashboardNames} from 'src/me/constants'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

interface StateProps {
  dashboards: Dashboard[]
  org: Organization
}

type Props = StateProps

const DashboardList: FC<Props> = ({dashboards, org}) => {
  if (dashboards && dashboards.length) {
    const sortedDashboards = sortBy(dashboards, ['meta.updatedAt']).reverse()

    return (
      <DapperScrollbars autoSizeHeight={true} style={{maxHeight: '400px'}}>
        <div className="recent-dashboards">
          {sortedDashboards.map(({id, name}) => (
            <Link
              key={id}
              to={`/orgs/${org.id}/dashboards/${id}`}
              className="recent-dashboards--item"
            >
              {name}
            </Link>
          ))}
        </div>
      </DapperScrollbars>
    )
  }

  return (
    <EmptyState size={ComponentSize.ExtraSmall}>
      <EmptyState.Text>You don't have any Dashboards</EmptyState.Text>
    </EmptyState>
  )
}

const mstp = (state: AppState): StateProps => {
  // map names and sort via a selector
  const dashboards = getSortedDashboardNames(
    getAll<Dashboard>(state, ResourceType.Dashboards)
  )

  return {
    dashboards: dashboards,
    org: getOrg(state),
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(DashboardList)
