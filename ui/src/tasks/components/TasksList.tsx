// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import TaskRow from 'src/tasks/components/TaskRow'

// Types
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'
import {Task as TaskAPI, User, Organization} from 'src/api'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
}

interface Props {
  tasks: Task[]
  searchTerm: string
  onActivate: (task: Task) => void
  onDelete: (task: Task) => void
  onCreate: () => void
  onSelect: (task: Task) => void
}

export default class TasksList extends PureComponent<Props> {
  public render() {
    const {searchTerm, onCreate} = this.props

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="NAME" width="20%" />
          <IndexList.HeaderCell columnName="ACTIVE" width="10%" />
          <IndexList.HeaderCell columnName="SCHEDULE" width="20%" />
          <IndexList.HeaderCell columnName="ORGANIZATION" width="15%" />
          <IndexList.HeaderCell columnName="" width="35%" />
        </IndexList.Header>
        <IndexList.Body
          emptyState={
            <EmptyTasksList searchTerm={searchTerm} onCreate={onCreate} />
          }
          columnCount={5}
        >
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {tasks, onActivate, onDelete, onSelect} = this.props

    return tasks.map(t => (
      <TaskRow
        key={`task-id--${t.id}`}
        task={t}
        onActivate={onActivate}
        onDelete={onDelete}
        onSelect={onSelect}
      />
    ))
  }
}
