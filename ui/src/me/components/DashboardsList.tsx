// Libraries
import React, {FC, useState, ChangeEvent} from 'react'
import {Link} from 'react-router-dom'
import {connect} from 'react-redux'
import {sortBy} from 'lodash'

// Components
import {
  EmptyState,
  DapperScrollbars,
  Input,
  IconFont,
  InputRef,
} from '@influxdata/clockface'

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
  const [searchTerm, setSearchTerm] = useState<string>('')

  const handleInputChange = (e: ChangeEvent<InputRef>): void => {
    setSearchTerm(e.target.value)
  }

  let dashboardsList = (
    <EmptyState size={ComponentSize.ExtraSmall}>
      <EmptyState.Text>You don't have any Dashboards</EmptyState.Text>
    </EmptyState>
  )

  if (dashboards && dashboards.length) {
    let recentlyModifiedDashboards = sortBy(dashboards, [
      'meta.updatedAt',
    ]).reverse()

    if (searchTerm.length) {
      recentlyModifiedDashboards = recentlyModifiedDashboards.filter(
        dashboard =>
          dashboard.name.toLowerCase().includes(searchTerm.toLowerCase())
      )
    }

    dashboardsList = (
      <DapperScrollbars autoSizeHeight={true} style={{maxHeight: '400px'}}>
        <div className="recent-dashboards">
          {recentlyModifiedDashboards.map(({id, name}) => (
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

    if (searchTerm && !recentlyModifiedDashboards.length) {
      dashboardsList = (
        <EmptyState size={ComponentSize.ExtraSmall}>
          <EmptyState.Text>
            No dashboards match <b>{searchTerm}</b>
          </EmptyState.Text>
        </EmptyState>
      )
    }
  }

  return (
    <>
      <Input
        className="recent-dashboards--filter"
        value={searchTerm}
        icon={IconFont.Search}
        placeholder="Filter dashboards..."
        onChange={handleInputChange}
      />
      {dashboardsList}
    </>
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

export default connect<StateProps, {}, {}>(mstp, null)(DashboardList)
