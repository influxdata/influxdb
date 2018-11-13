import React, {PureComponent} from 'react'

import {
  ComponentSpacer,
  Alignment,
  Button,
  ComponentColor,
  ComponentSize,
  SlideToggle,
} from 'src/clockface'
import IndexList from 'src/shared/components/index_views/IndexList'

import {Task, TaskStatus} from 'src/types/v2/tasks'

interface Props {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
}

export default class TaskRow extends PureComponent<Props> {
  public render() {
    const {task} = this.props

    return (
      <IndexList.Row disabled={this.disabled}>
        <IndexList.Cell>
          <a href="#" onClick={this.handleClick}>
            {task.name}
          </a>
        </IndexList.Cell>
        <IndexList.Cell>
          <SlideToggle
            active={!this.disabled}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
          />
        </IndexList.Cell>
        <IndexList.Cell>
          <a href="#">{task.organization.name}</a>
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Danger}
              text="Delete"
              onClick={this.handleDelete}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleClick = e => {
    e.preventDefault()

    this.props.onSelect(this.props.task)
  }

  private handleDelete = () => {
    this.props.onDelete(this.props.task)
  }

  private get disabled(): boolean {
    const {task} = this.props
    if (task.status === TaskStatus.Active) {
      return false
    }
    return true
  }

  private changeToggle = () => {
    const {task, onActivate} = this.props
    if (task.status === TaskStatus.Active) {
      task.status = TaskStatus.Inactive
    } else {
      task.status = TaskStatus.Active
    }
    onActivate(task)
  }
}
