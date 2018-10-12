import React, {PureComponent} from 'react'
import {Task} from 'src/types/v2/tasks'

import TaskRow from 'src/tasks/components/TaskRow'

interface Props {
  tasks: Task[]
  onDelete: (task: Task) => void
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
            <th style={{width: '140px'}} />
          </tr>
        </thead>
        <tbody>{this.tasks}</tbody>
      </table>
    )
  }

  private get tasks(): JSX.Element | JSX.Element[] {
    const {tasks, onDelete} = this.props

    if (tasks.length > 0) {
      return tasks.map(task => (
        <TaskRow key={`task-${task.id}`} onDelete={onDelete} task={task} />
      ))
    }

    return (
      <tr>
        <td style={{textAlign: 'center', padding: '15px'}} colSpan={4}>
          No tasks were found
        </td>
      </tr>
    )
  }
}
