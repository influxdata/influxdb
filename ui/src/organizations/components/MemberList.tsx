// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'

// Types
import {ResourceOwner} from 'src/api'

interface Props {
  members: ResourceOwner[]
  emptyState: JSX.Element
}

export default class MemberList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Username" width="25%" />
          <IndexList.HeaderCell columnName="Role" width="25%" />
          <IndexList.HeaderCell width="50%" />
        </IndexList.Header>
        <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    return this.props.members.map(member => (
      <IndexList.Row key={member.id}>
        <IndexList.Cell>{member.name}</IndexList.Cell>
        <IndexList.Cell>{member.role}</IndexList.Cell>
        <IndexList.Cell revealOnHover={true}>DELETE</IndexList.Cell>
      </IndexList.Row>
    ))
  }
}
