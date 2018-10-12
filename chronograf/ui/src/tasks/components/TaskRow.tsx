import React, {PureComponent} from 'react'

import {Task} from 'src/types/v2/tasks'
import {ComponentColor, ComponentSize, Button, SlideToggle} from 'src/clockface'
import {getDeep} from 'src/utils/wrappers'

interface Props {
  task: Task
}

export default class TaskRow extends PureComponent<Props> {
  public render() {
    return (
      <tr>
        <td>{this.name}</td>
        <td>{this.organizationName}</td>
        <td>
          <SlideToggle active={true} />
        </td>
        <td>
          <Button
            color={ComponentColor.Danger}
            text="Delete"
            size={ComponentSize.Medium}
          />
        </td>
      </tr>
    )
  }

  private get name(): string {
    const {task} = this.props

    return task.name
  }

  private get organizationName(): string {
    const {task} = this.props

    return getDeep(task, 'organization.name', '')
  }
}
