// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button, SlideToggle, ComponentSpacer} from '@influxdata/clockface'
import {IndexList, ConfirmationButton, Label} from 'src/clockface'
import FeatureFlag from 'src/shared/components/FeatureFlag'
import EditableName from 'src/shared/components/EditableName'

// Types
import {
  Stack,
  Alignment,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'
import {Task as TaskAPI} from '@influxdata/influx'
import {Task} from 'src/types/v2'

// Constants
import {DEFAULT_TASK_NAME} from 'src/dashboards/constants'
import {IconFont} from 'src/clockface/types/index'

interface Props {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onEditLabels: (task: Task) => void
  onRunTask: (taskID: string) => void
  onUpdate?: (task: Task) => void
  onFilterChange: (searchTerm: string) => void
}

export class TaskRow extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task, onDelete} = this.props

    return (
      <IndexList.Row disabled={!this.isTaskActive} testID="task-row">
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
        <IndexList.Cell>{task.latestCompleted}</IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              text="View Runs"
              onClick={this.handleViewRuns}
            />
            <FeatureFlag>
              <Button
                size={ComponentSize.ExtraSmall}
                text="Export"
                icon={IconFont.Export}
                onClick={this.handleExport}
              />
            </FeatureFlag>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Secondary}
              text="Clone"
              icon={IconFont.Duplicate}
              onClick={this.handleClone}
            />
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Primary}
              text="RunTask"
              onClick={this.handleRunTask}
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

  private handleRunTask = () => {
    this.props.onRunTask(this.props.task.id)
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
    const {router, task} = this.props
    router.push(`tasks/${task.id}/export`)
  }

  private handleClone = () => {
    const {task, onClone} = this.props
    onClone(task)
  }

  private handleOrgClick = () => {
    const {router, task} = this.props
    router.push(`/organizations/${task.organization.id}`)
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
        className="index-list--labels"
      >
        {task.labels.map(label => (
          <Label
            key={label.id}
            id={label.id}
            colorHex={label.properties.color}
            name={label.name}
            description={label.properties.description}
            onClick={this.handleLabelClick}
          />
        ))}
      </Label.Container>
    )
  }

  private handleLabelClick = (id: string) => {
    const label = this.props.task.labels.find(l => l.id === id)

    this.props.onFilterChange(label.name)
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
