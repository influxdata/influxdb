// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Types
import {Organization} from 'src/types/v2'

interface Props {
  orgs: Organization[]
}

export default class UserDashboardList extends PureComponent<Props> {
  public render() {
    const {orgs} = this.props

    if (this.isEmpty) {
      return <div>Looks like you dont belong to any organizations</div>
    }

    return (
      <ul className="link-list">
        {orgs.map(({id, name}) => (
          <li key={id}>
            <Link to={`/organizations/${id}/buckets_tab`}>{name}</Link>
          </li>
        ))}
      </ul>
    )
  }
  private get isEmpty(): boolean {
    return !this.props.orgs.length
  }
}
