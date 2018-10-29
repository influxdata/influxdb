import React, {PureComponent} from 'react'

import {
  ComponentSpacer,
  Alignment,
  Button,
  ComponentColor,
  ComponentSize,
} from 'src/clockface'
import IndexList from 'src/shared/components/index_views/IndexList'

import {Task} from 'src/types/v2/tasks'

interface Props {
  task: Task
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
}

export default class TaskRow extends PureComponent<Props> {
  public render() {
    const {task} = this.props

    return (
      <IndexList.Row>
        <IndexList.Cell>
          <a href="#" onClick={this.handleClick}>
            {task.name}
          </a>
        </IndexList.Cell>
        <IndexList.Cell>
          <a href="#">{task.organization.name}</a>
        </IndexList.Cell>
        <IndexList.Cell>Enabled</IndexList.Cell>
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
}
