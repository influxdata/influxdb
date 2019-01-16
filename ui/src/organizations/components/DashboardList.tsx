// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import DashboardRow from 'src/organizations/components/DashboardRow'

// Types
import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  emptyState: JSX.Element
  onDeleteDashboard: (dashboard: Dashboard) => void
}

export default class DashboardList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="name" width="50%" />
          <IndexList.HeaderCell columnName="modified" width="50%" />
        </IndexList.Header>
        <IndexList.Body columnCount={2} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {onDeleteDashboard} = this.props

    return this.props.dashboards.map(d => (
      <DashboardRow
        dashboard={d}
        key={d.id}
        onDeleteDashboard={onDeleteDashboard}
      />
    ))
  }
}
