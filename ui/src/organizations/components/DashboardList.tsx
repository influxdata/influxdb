// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'

// Types
import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
  emptyState: JSX.Element
}

export default class DashboardList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="75%" />
          <IndexList.HeaderCell width="25%" />
        </IndexList.Header>
        <IndexList.Body columnCount={2} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    return this.props.dashboards.map(dashboard => (
      <IndexList.Row key={dashboard.id}>
        <IndexList.Cell>
          <Link to={`/dashboards/${dashboard.id}`}>{dashboard.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell revealOnHover={true}>DELETE</IndexList.Cell>
      </IndexList.Row>
    ))
  }
}
