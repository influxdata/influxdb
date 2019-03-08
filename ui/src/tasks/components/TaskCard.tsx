// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {SlideToggle, ComponentSize} from '@influxdata/clockface'
import {ResourceList, Context} from 'src/clockface'
import FeatureFlag from 'src/shared/components/FeatureFlag'
import InlineLabels from 'src/shared/components/inlineLabels/InlineLabels'

// API
import {createLabelAJAX} from 'src/labels/api'

// Actions
import {addTaskLabelsAsync, removeTaskLabelsAsync} from 'src/tasks/actions/v2'

// Types
import {ComponentColor} from '@influxdata/clockface'
import {ITask as Task, ILabel} from '@influxdata/influx'
import {AppState, TaskStatus} from 'src/types/v2'

// Constants
import {DEFAULT_TASK_NAME} from 'src/dashboards/constants'
import {IconFont} from 'src/clockface/types/index'

interface PassedProps {
  task: Task
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onRunTask: (taskID: string) => void
  onUpdate: (task: Task) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  labels: ILabel[]
}

interface DispatchProps {
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
}

type Props = PassedProps & StateProps & DispatchProps

export class TaskCard extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {task} = this.props

    return (
      <ResourceList.Card
        testID="task-card"
        disabled={!this.isTaskActive}
        labels={() => this.labels}
        owner={{name: task.org, id: task.orgID}}
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

  private handleRenameTask = async (name: string) => {
    const {onUpdate, task} = this.props
    await onUpdate({...task, name})
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

  private handleAddLabel = (label: ILabel): void => {
    const {task, onAddTaskLabels} = this.props

    onAddTaskLabels(task.id, [label])
  }

  private handleRemoveLabel = (label: ILabel): void => {
    const {task, onRemoveTaskLabels} = this.props

    onRemoveTaskLabels(task.id, [label])
  }

  private handleCreateLabel = async (label: ILabel): Promise<ILabel> => {
    try {
      const newLabel = await createLabelAJAX(label)

      return newLabel
    } catch (err) {
      throw err
    }
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
    labels: labels.list,
  }
}

const mdtp: DispatchProps = {
  onAddTaskLabels: addTaskLabelsAsync,
  onRemoveTaskLabels: removeTaskLabelsAsync,
}

export default connect<StateProps, DispatchProps, PassedProps>(
  mstp,
  mdtp
)(withRouter<Props>(TaskCard))
