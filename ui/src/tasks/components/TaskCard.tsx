// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import {ResourceList, Label, Context} from 'src/clockface'
import FeatureFlag from 'src/shared/components/FeatureFlag'

// Types
import {ComponentColor} from '@influxdata/clockface'
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
  onUpdate: (task: Task) => void
  onFilterChange: (searchTerm: string) => void
}

export class TaskCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task} = this.props

    return (
      <ResourceList.Card
        testID="task-card"
        disabled={!this.isTaskActive}
        labels={() => this.labels}
        owner={task.organization}
        contextMenu={() => this.contextMenu}
        name={() => (
          <ResourceList.Name
            onEditName={this.handleNameClick}
            onUpdate={this.handleRenameTask}
            name={task.name}
            noNameString={DEFAULT_TASK_NAME}
            parentTestID="task-card--name"
            buttonTestID="task-card--name-button"
            inputTestID="task-card--input"
          />
        )}
        metaData={() => [
          <>Last completed at {task.latestCompleted}</>,
          <>{`Scheduled to run ${this.schedule}`}</>,
        ]}
        toggle={() => (
          <SlideToggle
            active={this.isTaskActive}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
            testID="task-card--slide-toggle"
          />
        )}
      />
    )
  }

  private get contextMenu(): JSX.Element {
    const {task, onClone, onDelete, onRunTask} = this.props

    return (
      <Context>
        <Context.Menu icon={IconFont.CogThick}>
          <FeatureFlag>
            <Context.Item label="Export" action={this.handleExport} />
          </FeatureFlag>
          <Context.Item label="View Task Runs" action={this.handleViewRuns} />
          <Context.Item label="Run Task" action={onRunTask} value={task.id} />
        </Context.Menu>
        <Context.Menu
          icon={IconFont.Duplicate}
          color={ComponentColor.Secondary}
        >
          <Context.Item label="Clone" action={onClone} value={task} />
        </Context.Menu>
        <Context.Menu
          icon={IconFont.Trash}
          color={ComponentColor.Danger}
          testID="context-delete-menu"
        >
          <Context.Item
            label="Delete"
            action={onDelete}
            value={task}
            testID="context-delete-task"
          />
        </Context.Menu>
      </Context>
    )
  }

  private handleNameClick = (e: MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault()

    this.props.onSelect(this.props.task)
  }

  private handleViewRuns = () => {
    const {router, task} = this.props
    router.push(`tasks/${task.id}/runs`)
  }

  private handleRenameTask = (name: string) => {
    const {onUpdate, task} = this.props
    onUpdate({...task, name})
  }

  private handleExport = () => {
    const {router, task} = this.props
    router.push(
      `/organizations/${task.organization.id}/tasks/${task.id}/export`
    )
  }

  private get labels(): JSX.Element {
    const {task} = this.props

    if (!task.labels.length) {
      return <Label.Container onEdit={this.handleEditLabels} />
    }

    return (
      <Label.Container resourceName="this Task" onEdit={this.handleEditLabels}>
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
      return `every ${task.every}, offset ${task.offset}`
    }
    if (task.every) {
      return `every ${task.every}`
    }
    if (task.cron) {
      return task.cron
    }
    return ''
  }
}
export default withRouter(TaskCard)
