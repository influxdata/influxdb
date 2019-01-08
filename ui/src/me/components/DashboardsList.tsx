// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {EmptyState, ComponentSize} from 'src/clockface'

// Types
import {Dashboard} from 'src/types/v2'

interface Props {
  dashboards: Dashboard[]
}

export default class UserDashboardList extends PureComponent<Props> {
  public render() {
    const {dashboards} = this.props

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
              <Link to={`/dashboards/${id}`}>{name}</Link>
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
