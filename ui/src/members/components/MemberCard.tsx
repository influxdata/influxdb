// Libraries
import React, {PureComponent} from 'react'

// Components
import {Member} from 'src/types'
import {ResourceCard} from '@influxdata/clockface'
import MemberContextMenu from 'src/members/components/MemberContextMenu'

interface Props {
  member: Member
  onDelete: (member: Member) => void
}

export default class MemberCard extends PureComponent<Props> {
  public render() {
    const {member, onDelete} = this.props

    return (
      <ResourceCard
        testID="task-card"
        contextMenu={<MemberContextMenu member={member} onDelete={onDelete} />}
      >
        <ResourceCard.Name name={member.name} />
        <ResourceCard.Meta>
          <>Role: {member.role}</>
        </ResourceCard.Meta>
      </ResourceCard>
    )
  }
}
