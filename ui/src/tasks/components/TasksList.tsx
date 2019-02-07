// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'
import TaskRow from 'src/tasks/components/TaskRow'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'
import {OverlayTechnology} from 'src/clockface'
import EditLabelsOverlay from 'src/shared/components/EditLabelsOverlay'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {Task as TaskAPI, User, Organization} from '@influxdata/influx'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
}
import {Sort} from 'src/clockface'
import {addTaskLabelsAsync, removeTaskLabelsAsync} from 'src/tasks/actions/v2'

interface Props {
  tasks: Task[]
  searchTerm: string
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onCreate: () => void
  onSelect: (task: Task) => void
  onClone: (task: Task) => void
  totalCount: number
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onAddTaskLabels: typeof addTaskLabelsAsync
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
    const {searchTerm, onCreate, totalCount} = this.props
    const {sortKey, sortDirection} = this.state

    const headerKeys: SortKey[] = [
      'name',
      'status',
      'every',
      'organization.name',
    ]

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              columnName="Name"
              width="55%"
              sortKey={headerKeys[0]}
              sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <IndexList.HeaderCell
              columnName="Owner"
              width="15%"
              sortKey={headerKeys[3]}
              sort={sortKey === headerKeys[3] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <IndexList.HeaderCell
              columnName="Active"
              width="5%"
              sortKey={headerKeys[1]}
              sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <IndexList.HeaderCell
              columnName="Schedule"
              width="15%"
              sortKey={headerKeys[2]}
              sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
              onClick={this.handleClickColumn}
            />
            <IndexList.HeaderCell columnName="" width="10%" />
          </IndexList.Header>
          <IndexList.Body
            emptyState={
              <EmptyTasksList
                searchTerm={searchTerm}
                onCreate={onCreate}
                totalCount={totalCount}
              />
            }
            columnCount={5}
          >
            {this.sortedRows}
          </IndexList.Body>
        </IndexList>
        {this.renderLabelEditorOverlay}
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private rows = (tasks: Task[]): JSX.Element => {
    const {onActivate, onDelete, onSelect, onClone} = this.props
    const taskrows = (
      <>
        {tasks.map(t => (
          <TaskRow
            key={`task-id--${t.id}`}
            task={t}
            onActivate={onActivate}
            onDelete={onDelete}
            onClone={onClone}
            onSelect={onSelect}
            onEditLabels={this.handleStartEditingLabels}
          />
        ))}
      </>
    )
    return taskrows
  }

  private get sortedRows(): JSX.Element {
    const {tasks} = this.props
    const {sortKey, sortDirection} = this.state

    if (tasks.length) {
      return (
        <SortingHat<Task>
          list={tasks}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {this.rows}
        </SortingHat>
      )
    }

    return null
  }

  private handleStartEditingLabels = (taskLabelsEdit: Task): void => {
    this.setState({taskLabelsEdit, isEditingTaskLabels: true})
  }

  private handleStopEditingLabels = (): void => {
    this.setState({isEditingTaskLabels: false})
  }

  private get renderLabelEditorOverlay(): JSX.Element {
    const {onAddTaskLabels, onRemoveTaskLabels} = this.props
    const {isEditingTaskLabels, taskLabelsEdit} = this.state

    return (
      <OverlayTechnology visible={isEditingTaskLabels}>
        <EditLabelsOverlay<Task>
          resource={taskLabelsEdit}
          onDismissOverlay={this.handleStopEditingLabels}
          onAddLabels={onAddTaskLabels}
          onRemoveLabels={onRemoveTaskLabels}
        />
      </OverlayTechnology>
    )
  }
}
