// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Types
import {Dashboard} from 'src/types'

interface Props {
  dashboards: Dashboard[]
}

export default class UserDashboardList extends PureComponent<Props> {
  public render() {
    const {dashboards} = this.props
    if (this.isEmpty) {
      return <div>Looks like you dont have any dashboards</div>
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
