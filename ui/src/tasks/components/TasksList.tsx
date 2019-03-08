// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ResourceList} from 'src/clockface'
import TaskCard from 'src/tasks/components/TaskCard'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {ITask as Task} from '@influxdata/influx'

import {Sort} from 'src/clockface'
import {
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
  runTask,
} from 'src/tasks/actions/v2'

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
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRunTask: typeof runTask
  onUpdate: (task: Task) => void
  filterComponent?: () => JSX.Element
}

type SortKey = keyof Task | 'organization.name'

interface State {
  sortKey: SortKey
  sortDirection: Sort
  taskLabelsEdit: Task
  isEditingTaskLabels: boolean
}

export default class TasksList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: null,
      sortDirection: Sort.Descending,
      taskLabelsEdit: null,
      isEditingTaskLabels: false,
    }
  }

  public render() {
    const {searchTerm, onCreate, totalCount, filterComponent} = this.props
    const {sortKey, sortDirection} = this.state

    const headerKeys: SortKey[] = [
      'name',
      'status',
      'every',
      'organization.name',
      'latestCompleted',
    ]

    return (
      <>
        <ResourceList>
          <ResourceList.Header filterComponent={filterComponent}>
            <ResourceList.Sorter
              name="Name"
              sortKey={headerKeys[0]}
              sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <ResourceList.Sorter
              name="Owner"
              sortKey={headerKeys[3]}
              sort={sortKey === headerKeys[3] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <ResourceList.Sorter
              name="Active"
              sortKey={headerKeys[1]}
              sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <ResourceList.Sorter
              name="Schedule"
              sortKey={headerKeys[2]}
              sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <ResourceList.Sorter
              name="Last Completed"
              sortKey={headerKeys[4]}
              sort={sortKey === headerKeys[4] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body
            emptyState={
              <EmptyTasksList
                searchTerm={searchTerm}
                onCreate={onCreate}
                totalCount={totalCount}
              />
            }
          >
            {this.sortedCards}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private cards = (tasks: Task[]): JSX.Element => {
    const {
      onActivate,
      onDelete,
      onSelect,
      onClone,
      onUpdate,
      onRunTask,
      onFilterChange,
    } = this.props
    const taskCards = (
      <>
        {tasks.map(t => (
          <TaskCard
            key={`task-id--${t.id}`}
            task={t}
            onActivate={onActivate}
            onDelete={onDelete}
            onClone={onClone}
            onSelect={onSelect}
            onUpdate={onUpdate}
            onRunTask={onRunTask}
            onFilterChange={onFilterChange}
          />
        ))}
      </>
    )
    return taskCards
  }

  private get sortedCards(): JSX.Element {
    const {tasks} = this.props
    const {sortKey, sortDirection} = this.state

    if (tasks.length) {
      return (
        <SortingHat<Task>
          list={tasks}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {this.cards}
        </SortingHat>
      )
    }

    return null
  }
}
