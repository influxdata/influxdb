// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {EmptyState, ComponentSize} from 'src/clockface'

// Types
import {Organization} from 'src/types'

interface Props {
  orgs: Organization[]
}

export default class UserDashboardList extends PureComponent<Props> {
  public render() {
    const {orgs} = this.props

    if (this.isEmpty) {
      return (
        <EmptyState size={ComponentSize.ExtraSmall}>
          <EmptyState.Text text="You don't belong to any Organizations" />
        </EmptyState>
      )
    }

    return (
      <ul className="link-list">
        {orgs.map(({id, name}) => (
          <li key={id}>
            <Link to={`/organizations/${id}/buckets`}>{name}</Link>
          </li>
        ))}
      </ul>
    )
  }
  private get isEmpty(): boolean {
    return !this.props.orgs.length
  }
}
