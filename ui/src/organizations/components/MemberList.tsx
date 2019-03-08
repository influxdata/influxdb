// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import MemberRow from 'src/organizations/components/MemberRow'

// Types
import {ResourceOwner} from '@influxdata/influx'

interface Props {
  members: ResourceOwner[]
  emptyState: JSX.Element
  onDelete: (member: ResourceOwner) => void
}

export default class MemberList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Username" width="20%" />
          <IndexList.HeaderCell columnName="Role" width="20%" />
          <IndexList.HeaderCell width="60%" />
        </IndexList.Header>
        <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {members, onDelete} = this.props

    return members.map(member => (
      <MemberRow key={member.id} member={member} onDelete={onDelete} />
    ))
  }
}
