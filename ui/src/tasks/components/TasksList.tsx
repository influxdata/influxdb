// Libraries
import React, {PureComponent} from 'react'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'
import TaskRow from 'src/tasks/components/TaskRow'

// Types
import {Task} from 'src/types/v2/tasks'
import EmptyTasksList from 'src/tasks/components/EmptyTasksList'

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
          <IndexList.HeaderCell columnName="ORGANIZATION" width="15%" />
          <IndexList.HeaderCell columnName="" width="50%" />
        </IndexList.Header>
        <IndexList.Body
          emptyState={
            <EmptyTasksList searchTerm={searchTerm} onCreate={onCreate} />
          }
          columnCount={4}
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
