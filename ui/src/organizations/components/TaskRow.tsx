// Libraries
import React, {PureComponent} from 'react'
import {Link} from 'react-router'

// Components
import {
  ComponentSize,
  IndexList,
  ConfirmationButton,
  Alignment,
} from 'src/clockface'

// Api
import {Task} from 'src/api'

interface Props {
  task: Task
  onDelete: (taskID: string) => void
}

export default class TaskRow extends PureComponent<Props> {
  public render() {
    const {task} = this.props

    return (
      <>
        <IndexList.Row key={task.id}>
          <IndexList.Cell>
            <Link to={`/tasks/${task.id}`}>{task.name}</Link>
          </IndexList.Cell>
          <IndexList.Cell>{task.owner.name}</IndexList.Cell>
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

  private handleDeleteTask = (): void => {
    this.props.onDelete(this.props.task.id)
  }
}
