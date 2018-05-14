import React, {PureComponent, SFC} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import {AlertRule, Source} from 'src/types'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import {TASKS_TABLE} from 'src/kapacitor/constants/tableSizing'
const {colName, colType, colEnabled, colActions} = TASKS_TABLE

interface TasksTableProps {
  tasks: AlertRule[]
  source: Source
  onChangeRuleStatus: (rule: AlertRule) => void
  onDelete: (rule: AlertRule) => void
}

interface TaskRowProps {
  task: AlertRule
  source: Source
  onChangeRuleStatus: (rule: AlertRule) => void
  onDelete: (rule: AlertRule) => void
}

const TasksTable: SFC<TasksTableProps> = ({
  tasks,
  source,
  onDelete,
  onChangeRuleStatus,
}) => (
  <table className="table v-center table-highlight">
    <thead>
      <tr>
        <th style={{minWidth: colName}}>Name</th>
        <th style={{width: colType}}>Type</th>
        <th style={{width: colEnabled}} className="text-center">
          Task Enabled
        </th>
        <th style={{width: colActions}} />
      </tr>
    </thead>
    <tbody>
      {_.sortBy(tasks, t => t.id.toLowerCase()).map(task => {
        return (
          <TaskRow
            key={task.id}
            task={task}
            source={source}
            onDelete={onDelete}
            onChangeRuleStatus={onChangeRuleStatus}
          />
        )
      })}
    </tbody>
  </table>
)

export class TaskRow extends PureComponent<TaskRowProps> {
  public render() {
    const {task, source} = this.props

    return (
      <tr key={task.id}>
        <td style={{minWidth: colName}}>
          <Link
            className="link-success"
            to={`/sources/${source.id}/tickscript/${task.id}`}
          >
            {task.name}
          </Link>
        </td>
        <td style={{width: colType, textTransform: 'capitalize'}}>
          {task.type}
        </td>
        <td style={{width: colEnabled}} className="text-center">
          <div className="dark-checkbox">
            <input
              id={`kapacitor-task-row-task-enabled ${task.id}`}
              className="form-control-static"
              type="checkbox"
              checked={task.status === 'enabled'}
              onChange={this.handleClickRuleStatusEnabled}
            />
            <label htmlFor={`kapacitor-task-row-task-enabled ${task.id}`} />
          </div>
        </td>
        <td style={{width: colActions}} className="text-right">
          <ConfirmButton
            text="Delete"
            type="btn-danger"
            size="btn-xs"
            customClass="table--show-on-row-hover"
            confirmAction={this.handleDelete}
          />
        </td>
      </tr>
    )
  }

  private handleDelete = () => {
    const {onDelete, task} = this.props

    onDelete(task)
  }

  private handleClickRuleStatusEnabled = () => {
    const {onChangeRuleStatus, task} = this.props

    onChangeRuleStatus(task)
  }
}

export default TasksTable
