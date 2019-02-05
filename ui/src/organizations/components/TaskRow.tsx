// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
} from 'src/clockface'

// Api
import {Task} from 'src/api'
import EditableName from 'src/shared/components/EditableName'

interface Props {
  task: Task
  onDelete: (taskID: string) => void
  onUpdate: (task: Task) => void
}

export default class TaskRow extends PureComponent<Props> {
  public render() {
    const {task} = this.props

    return (
      <>
        <IndexList.Row key={task.id}>
          <IndexList.Cell>
            <EditableName
              onUpdate={this.handleUpdateTask}
              name={task.name}
              hrefValue={`/tasks/${task.id}`}
            />
          </IndexList.Cell>
          <IndexList.Cell>{task.name}</IndexList.Cell>
          <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={this.handleDeleteTask}
            />
          </IndexList.Cell>
        </IndexList.Row>
      </>
    )
  }

  private handleUpdateTask = (name: string) => {
    const {onUpdate, task} = this.props
    onUpdate({...task, name})
  }

  private handleDeleteTask = (): void => {
    this.props.onDelete(this.props.task.id)
  }
}
