// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'
import moment from 'moment'

// Components
import {IndexList} from 'src/clockface'

// Types
import {Dashboard} from 'src/types/v2'

// Constants
import {UPDATED_AT_TIME_FORMAT} from 'src/dashboards/constants'

interface Props {
  dashboards: Dashboard[]
  emptyState: JSX.Element
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
    return this.props.dashboards.map(d => (
      <IndexList.Row key={d.id}>
        <IndexList.Cell>
          <Link to={`/dashboards/${d.id}`}>{d.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell revealOnHover={true}>
          {moment(d.meta.updatedAt).format(UPDATED_AT_TIME_FORMAT)}
        </IndexList.Cell>
      </IndexList.Row>
    ))
  }
}
