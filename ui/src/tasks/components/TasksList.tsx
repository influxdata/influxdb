// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'
import TaskRow from 'src/tasks/components/TaskRow'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {Task as TaskAPI, User, Organization} from 'src/api'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
}
import {Sort} from 'src/clockface'

interface Props {
  tasks: Task[]
  searchTerm: string
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onCreate: () => void
  onSelect: (task: Task) => void
  totalCount: number
}

type SortKey = keyof Task | 'organization.name'

interface State {
  sortKey: SortKey
  sortDirection: Sort
}

export default class TasksList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: null,
      sortDirection: Sort.Descending,
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
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            columnName="name"
            width="20%"
            sortKey={headerKeys[0]}
            sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="active"
            width="10%"
            sortKey={headerKeys[1]}
            sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="schedule"
            width="20%"
            sortKey={headerKeys[2]}
            sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="organization"
            width="15%"
            sortKey={headerKeys[3]}
            sort={sortKey === headerKeys[3] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell columnName="" width="35%" />
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
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  private rows = (tasks: Task[]): JSX.Element => {
    const {onActivate, onDelete, onSelect} = this.props
    const taskrows = (
      <>
        {tasks.map(t => (
          <TaskRow
            key={`task-id--${t.id}`}
            task={t}
            onActivate={onActivate}
            onDelete={onDelete}
            onSelect={onSelect}
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
}
