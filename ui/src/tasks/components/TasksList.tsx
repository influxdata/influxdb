// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import TaskCard from 'src/tasks/components/TaskCard'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {Task} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

import {
  addTaskLabelAsync,
  removeTaskLabelAsync,
  runTask,
} from 'src/tasks/actions'
import {checkTaskLimits as checkTaskLimitsAction} from 'src/cloud/actions/limits'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

interface Props {
  tasks: Task[]
  searchTerm: string
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onCreate: () => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  onFilterChange: (searchTerm: string) => void
  totalCount: number
  onRemoveTaskLabel: typeof removeTaskLabelAsync
  onAddTaskLabel: typeof addTaskLabelAsync
  onRunTask: typeof runTask
  onUpdate: (name: string, taskID: string) => void
  filterComponent?: JSX.Element
  onImportTask: () => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
  checkTaskLimits: typeof checkTaskLimitsAction
  onImportFromTemplate: () => void
}

type SortKey = keyof Task

interface State {
  taskLabelsEdit: Task
  isEditingTaskLabels: boolean
}

export default class TasksList extends PureComponent<Props, State> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  constructor(props) {
    super(props)
    this.state = {
      taskLabelsEdit: null,
      isEditingTaskLabels: false,
    }
  }

  public componentDidMount() {
    this.props.checkTaskLimits()
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
      onImportFromTemplate,
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
                onImportFromTemplate={onImportFromTemplate}
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
      tasks,
      sortKey,
      sortDirection,
      sortType,
      onActivate,
      onDelete,
      onSelect,
      onClone,
      onUpdate,
      onRunTask,
      onFilterChange,
    } = this.props

    const sortedTasks = this.memGetSortedResources(
      tasks,
      sortKey,
      sortDirection,
      sortType
    )

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
