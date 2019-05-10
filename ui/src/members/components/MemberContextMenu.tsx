// Libraries
import React, {PureComponent} from 'react'

// Components
import {Member} from 'src/types'
import {Context} from 'src/clockface'

import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import {IconFont, ComponentColor} from '@influxdata/clockface'

interface Props {
  member: Member
  onDelete: (member: Member) => void
}

export default class MemberContextMenu extends PureComponent<Props> {
  public render() {
    const {member} = this.props

    return (
      <CloudExclude>
        <Context>
          <Context.Menu
            icon={IconFont.Trash}
            color={ComponentColor.Danger}
            testID="context-delete-menu"
          >
            <Context.Item
              label="Delete"
              action={this.handleDelete}
              value={member}
              testID="context-delete-task"
            />
          </Context.Menu>
        </Context>
      </CloudExclude>
    )
  }

  private handleDelete = () => {
    const {member, onDelete} = this.props
    onDelete(member)
  }
}
