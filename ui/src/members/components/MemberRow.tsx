// Libraries
import React, {PureComponent} from 'react'

// Components
import {Member} from 'src/types'
import {
  IndexList,
  ConfirmationButton,
  ComponentSize,
  Alignment,
} from 'src/clockface'

import CloudExclude from 'src/shared/components/cloud/CloudExclude'

interface Props {
  member: Member
  onDelete: (member: Member) => void
}

export default class MemberRow extends PureComponent<Props> {
  public render() {
    const {member} = this.props

    return (
      <IndexList.Row key={member.id}>
        <IndexList.Cell>{member.name}</IndexList.Cell>
        <IndexList.Cell>{member.role}</IndexList.Cell>
        <CloudExclude>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={this.handleDelete}
            />
          </IndexList.Cell>
        </CloudExclude>
      </IndexList.Row>
    )
  }

  private handleDelete = () => {
    const {member, onDelete} = this.props
    onDelete(member)
  }
}
