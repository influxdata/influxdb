// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import TaskCard from 'src/tasks/components/TaskCard'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {ITask as Task} from '@influxdata/influx'
import {SortTypes} from 'src/shared/selectors/sort'
import {AppState} from 'src/types'
import {Sort} from '@influxdata/clockface'

import {
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
  runTask,
} from 'src/tasks/actions'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

interface OwnProps {
  tasks: Task[]
  searchTerm: string
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onCreate: () => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onFilterChange: (searchTerm: string) => void
  totalCount: number
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRunTask: typeof runTask
  onUpdate: (task: Task) => void
  filterComponent?: () => JSX.Element
  onImportTask: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedTasks: Task[]
}

type Props = OwnProps & StateProps

type SortKey = keyof Task

interface State {
  taskLabelsEdit: Task
  isEditingTaskLabels: boolean
  sortedTasks: Task[]
}

class TasksList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      taskLabelsEdit: null,
      isEditingTaskLabels: false,
      sortedTasks: this.props.sortedTasks,
    }
  }

  componentDidUpdate(prevProps) {
    const {tasks, sortedTasks, sortKey, sortDirection} = this.props
    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.tasks.length !== tasks.length
    ) {
      this.setState({sortedTasks})
    }
  }

  public render() {
    const {
      searchTerm,
      onCreate,
      totalCount,
      filterComponent,
      onImportTask,
      sortKey,
      sortDirection,
      onClickColumn,
    } = this.props

    const headerKeys: SortKey[] = ['name', 'status', 'every', 'latestCompleted']

    return (
      <>
        <ResourceList>
          <ResourceList.Header filterComponent={filterComponent}>
            <ResourceList.Sorter
              name="Name"
              sortKey={headerKeys[0]}
              sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
            <ResourceList.Sorter
              name="Active"
              sortKey={headerKeys[1]}
              sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
            <ResourceList.Sorter
              name="Schedule"
              sortKey={headerKeys[2]}
              sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
            <ResourceList.Sorter
              name="Last Completed"
              sortKey={headerKeys[3]}
              sort={sortKey === headerKeys[3] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body
            emptyState={
              <EmptyTasksList
                searchTerm={searchTerm}
                onCreate={onCreate}
                totalCount={totalCount}
                onImportTask={onImportTask}
              />
            }
          >
            {this.rows}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get rows(): JSX.Element[] {
    const {
      onActivate,
      onDelete,
      onSelect,
      onClone,
      onUpdate,
      onRunTask,
      onFilterChange,
    } = this.props
    const {sortedTasks} = this.state

    return sortedTasks.map(task => (
      <TaskCard
        key={`task-id--${task.id}`}
        task={task}
        onActivate={onActivate}
        onDelete={onDelete}
        onClone={onClone}
        onSelect={onSelect}
        onUpdate={onUpdate}
        onRunTask={onRunTask}
        onFilterChange={onFilterChange}
      />
    ))
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedTasks: getSortedResource(state.tasks.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(TasksList)
