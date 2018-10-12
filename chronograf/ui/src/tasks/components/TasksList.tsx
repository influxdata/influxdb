import React, {PureComponent} from 'react'
import {Task} from 'src/types/v2/tasks'

import TaskRow from 'src/tasks/components/TaskRow'

interface Props {
  tasks: Task[]
}

export default class TasksList extends PureComponent<Props> {
  public render() {
    return (
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Organization</th>
            <th>Enabled</th>
            <th />
          </tr>
        </thead>
        <tbody>{this.tasks}</tbody>
      </table>
    )
  }

  private get tasks(): JSX.Element[] {
    const {tasks} = this.props

    return tasks.map(task => <TaskRow key={`task-${task.id}`} task={task} />)
  }
}
