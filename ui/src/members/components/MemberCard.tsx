// Libraries
import React, {PureComponent} from 'react'

// Components
import {Member} from 'src/types'
import {ResourceList} from 'src/clockface'
import MemberContextMenu from 'src/members/components/MemberContextMenu'

interface Props {
  member: Member
  onDelete: (member: Member) => void
}

export default class MemberCard extends PureComponent<Props> {
  public render() {
    const {member, onDelete} = this.props

    return (
      <>
        <ResourceList.Card
          testID="task-card"
          contextMenu={() => (
            <MemberContextMenu member={member} onDelete={onDelete} />
          )}
          name={() => <ResourceList.Name name={member.name} />}
          metaData={() => [<>Role: {member.role}</>]}
        />
      </>
    )
  }
}
