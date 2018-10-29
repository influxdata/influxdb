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
          <IndexList.HeaderCell columnName="Name" width="65%" />
          <IndexList.HeaderCell columnName="Organization" width="15%" />
          <IndexList.HeaderCell columnName="Status" width="10%" />
          <IndexList.HeaderCell columnName="" width="10%" />
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
    const {tasks, onDelete, onSelect} = this.props

    return tasks.map(t => (
      <TaskRow
        key={`task-id--${t.id}`}
        task={t}
        onDelete={onDelete}
        onSelect={onSelect}
      />
    ))
  }
}
