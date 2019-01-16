// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'

// Types
import {Task} from 'src/api'
import TaskRow from 'src/organizations/components/TaskRow'

interface Props {
  tasks: Task[]
  emptyState: JSX.Element
  onDelete: (taskID: string) => void
}

export default class TaskList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" width="40%" />
          <IndexList.HeaderCell columnName="Owner" width="40%" />
          <IndexList.HeaderCell width="20%" />
        </IndexList.Header>
        <IndexList.Body columnCount={3} emptyState={this.props.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {tasks, onDelete} = this.props
    return tasks.map(task => (
      <TaskRow key={task.id} task={task} onDelete={onDelete} />
    ))
  }
}
