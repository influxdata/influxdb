// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  ComponentSpacer,
  ComponentColor,
  Alignment,
  Button,
  ComponentSize,
  SlideToggle,
  IndexList,
  ConfirmationButton,
  Stack,
  Label,
} from 'src/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'
import {Task as TaskAPI, Organization} from '@influxdata/influx'

// Constants
import {DEFAULT_TASK_NAME} from 'src/dashboards/constants'

interface Task extends TaskAPI {
  organization: Organization
}

// Constants
import {IconFont} from 'src/clockface/types/index'
import EditableName from 'src/shared/components/EditableName'

interface Props {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onEditLabels: (task: Task) => void
  onUpdate?: (task: Task) => void
}

export class TaskRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task, onDelete} = this.props

    return (
      <IndexList.Row disabled={!this.isTaskActive}>
        <IndexList.Cell>
          <ComponentSpacer
            stackChildren={Stack.Columns}
            align={Alignment.Right}
          >
            {this.resourceNames}
            {this.labels}
          </ComponentSpacer>
        </IndexList.Cell>
        <IndexList.Cell>
          <a href="" onClick={this.handleOrgClick}>
            {task.organization.name}
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
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="View Runs"
              onClick={this.handleViewRuns}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              text="Export"
              icon={IconFont.Export}
              onClick={this.handleExport}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text="Clone"
              icon={IconFont.Duplicate}
              onClick={this.handleClone}
            />
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={onDelete}
              returnValue={task}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private get resourceNames(): JSX.Element {
    const {onUpdate, task} = this.props
    if (onUpdate) {
      return (
        <EditableName
          onUpdate={this.handleUpdateTask}
          name={task.name}
          onEditName={this.handleClick}
          noNameString={DEFAULT_TASK_NAME}
        />
      )
    }
    return (
      <a
        href="#"
        onClick={this.handleClick}
        className="index-list--resource-name"
      >
        {task.name}
      </a>
    )
  }

  private handleViewRuns = () => {
    const {router, task} = this.props
    router.push(`tasks/${task.id}/runs`)
  }

  private handleUpdateTask = (name: string) => {
    const {onUpdate, task} = this.props
    onUpdate({...task, name})
  }

  private handleClick = e => {
    e.preventDefault()

    this.props.onSelect(this.props.task)
  }

  private handleExport = () => {
    const {task} = this.props
    downloadTextFile(task.flux, `${task.name}.flux`)
  }

  private handleClone = () => {
    const {task, onClone} = this.props
    onClone(task)
  }

  private handleOrgClick = () => {
    const {router, task} = this.props
    router.push(`/organizations/${task.organization.id}/members_tab`)
  }

  private get labels(): JSX.Element {
    const {task} = this.props

    if (!task.labels.length) {
      return (
        <Label.Container
          limitChildCount={4}
          className="index-list--labels"
          onEdit={this.handleEditLabels}
        />
      )
    }

    return (
      <Label.Container
        limitChildCount={4}
        resourceName="this Task"
        onEdit={this.handleEditLabels}
      >
        {task.labels.map(label => (
          <Label
            key={label.id}
            id={label.id}
            colorHex={label.properties.color}
            name={label.name}
            description={label.properties.description}
          />
        ))}
      </Label.Container>
    )
  }

  private get isTaskActive(): boolean {
    const {task} = this.props
    if (task.status === TaskAPI.StatusEnum.Active) {
      return true
    }
    return false
  }

  private handleEditLabels = () => {
    const {task, onEditLabels} = this.props

    onEditLabels(task)
  }

  private changeToggle = () => {
    const {task, onActivate} = this.props
    if (task.status === TaskAPI.StatusEnum.Active) {
      task.status = TaskAPI.StatusEnum.Inactive
    } else {
      task.status = TaskAPI.StatusEnum.Active
    }
    onActivate(task)
  }

  private get schedule(): string {
    const {task} = this.props
    if (task.every && task.offset) {
      return `Every ${task.every}, Offset ${task.offset}`
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
