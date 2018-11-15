// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import download from 'src/external/download'

// Components
import {
  ComponentSpacer,
  Alignment,
  Button,
  ComponentColor,
  ComponentSize,
  SlideToggle,
  IndexList,
} from 'src/clockface'

// Types
import {Task, TaskStatus} from 'src/types/v2/tasks'

interface Props {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
}

class TaskRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task} = this.props
    return (
      <IndexList.Row disabled={!this.isTaskActive}>
        <IndexList.Cell>
          <a href="#" onClick={this.handleClick}>
            {task.name}
          </a>
        </IndexList.Cell>
        <IndexList.Cell>
          <SlideToggle
            active={this.isTaskActive}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
          />
        </IndexList.Cell>
        <IndexList.Cell>{this.schedule}</IndexList.Cell>
        <IndexList.Cell>
          <a href="" onClick={this.handleOrgClick}>
            {task.organization.name}
          </a>
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="Export"
              onClick={this.handleExport}
            />
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

  private handleExport = () => {
    const {task} = this.props
    download(task.flux, `${task.name}.flux`, 'text/plain')
  }

  private handleOrgClick = () => {
    const {router, task} = this.props
    router.push(`/organizations/${task.organization.id}/members_tab`)
  }

  private get isTaskActive(): boolean {
    const {task} = this.props
    if (task.status === TaskStatus.Active) {
      return true
    }
    return false
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

  private get schedule(): string {
    const {task} = this.props
    if (task.every && task.delay) {
      return `Every ${task.every}, Delay ${task.delay}`
    }
    if (task.every) {
      return `Every ${task.every}`
    }
    if (task.cron) {
      return task.cron
    }
    return ''
  }
}
export default withRouter(TaskRow)
