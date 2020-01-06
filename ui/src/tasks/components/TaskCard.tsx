// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {
  SlideToggle,
  ComponentSize,
  ResourceCard,
  IconFont,
  InputLabel,
  FlexBox,
} from '@influxdata/clockface'
import {Context} from 'src/clockface'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'
import LastRunTaskStatus from 'src/shared/components/lastRunTaskStatus/LastRunTaskStatus'

// Actions
import {addTaskLabelAsync, removeTaskLabelAsync} from 'src/tasks/actions'
import {createLabel as createLabelAsync} from 'src/labels/actions'

// Selectors
import {viewableLabels} from 'src/labels/selectors'

// Types
import {ComponentColor} from '@influxdata/clockface'
import {AppState, Task, Label} from 'src/types'

// Constants
import {DEFAULT_TASK_NAME} from 'src/dashboards/constants'

interface PassedProps {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onRunTask: (taskID: string) => void
  onUpdate: (name: string, taskID: string) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: Label[]
}

interface DispatchProps {
  onAddTaskLabel: typeof addTaskLabelAsync
  onRemoveTaskLabel: typeof removeTaskLabelAsync
  onCreateLabel: typeof createLabelAsync
}

type Props = PassedProps & StateProps & DispatchProps

export class TaskCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task} = this.props

    return (
      <ResourceCard
        testID="task-card"
        disabled={!this.isTaskActive}
        labels={this.labels}
        contextMenu={this.contextMenu}
        name={
          <ResourceCard.EditableName
            onClick={this.handleNameClick}
            onUpdate={this.handleRenameTask}
            name={task.name}
            noNameString={DEFAULT_TASK_NAME}
            testID="task-card--name"
            buttonTestID="task-card--name-button"
            inputTestID="task-card--input"
          />
        }
        metaData={[
          this.activeToggle,
          <>Last completed at {task.latestCompleted}</>,
          <>{`Scheduled to run ${this.schedule}`}</>,
        ]}
        toggle={
          <LastRunTaskStatus
            lastRunError={task.lastRunError}
            lastRunStatus={task.lastRunStatus}
          />
        }
      />
    )
  }

  private get activeToggle(): JSX.Element {
    const labelText = this.isTaskActive ? 'Active' : 'Inactive'
    return (
      <FlexBox margin={ComponentSize.Small}>
        <SlideToggle
          active={this.isTaskActive}
          size={ComponentSize.ExtraSmall}
          onChange={this.changeToggle}
          testID="task-card--slide-toggle"
        />
        <InputLabel active={this.isTaskActive}>{labelText}</InputLabel>
      </FlexBox>
    )
  }

  private get contextMenu(): JSX.Element {
    const {task, onClone, onDelete, onRunTask} = this.props

    return (
      <Context>
        <Context.Menu icon={IconFont.CogThick}>
          <Context.Item label="Export" action={this.handleExport} />
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

  private handleNameClick = (e: MouseEvent) => {
    e.preventDefault()

    this.props.onSelect(this.props.task)
  }

  private handleViewRuns = () => {
    const {
      router,
      task,
      params: {orgID},
    } = this.props
    router.push(`/orgs/${orgID}/tasks/${task.id}/runs`)
  }

  private handleRenameTask = (name: string) => {
    const {
      onUpdate,
      task: {id},
    } = this.props
    onUpdate(name, id)
  }

  private handleExport = () => {
    const {
      router,
      task,
      location: {pathname},
    } = this.props
    router.push(`${pathname}/${task.id}/export`)
  }

  private get labels(): JSX.Element {
    const {task, labels, onFilterChange} = this.props

    return (
      <InlineLabels
        selectedLabels={task.labels}
        labels={labels}
        onFilterChange={onFilterChange}
        onAddLabel={this.handleAddLabel}
        onRemoveLabel={this.handleRemoveLabel}
        onCreateLabel={this.handleCreateLabel}
      />
    )
  }

  private handleAddLabel = (label: Label) => {
    const {task, onAddTaskLabel} = this.props

    onAddTaskLabel(task.id, label)
  }

  private handleRemoveLabel = (label: Label) => {
    const {task, onRemoveTaskLabel} = this.props

    onRemoveTaskLabel(task.id, label)
  }

  private handleCreateLabel = (label: Label) => {
    this.props.onCreateLabel(label.name, label.properties)
  }

  private get isTaskActive(): boolean {
    const {task} = this.props
    if (task.status === 'active') {
      return true
    }
    return false
  }

  private changeToggle = () => {
    const {task, onActivate} = this.props
    if (task.status === 'active') {
      task.status = 'inactive'
    } else {
      task.status = 'active'
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

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels: viewableLabels(labels.list),
  }
}

const mdtp: DispatchProps = {
  onCreateLabel: createLabelAsync,
  onAddTaskLabel: addTaskLabelAsync,
  onRemoveTaskLabel: removeTaskLabelAsync,
}

export default connect<StateProps, DispatchProps, PassedProps>(
  mstp,
  mdtp
)(withRouter<Props>(TaskCard))
