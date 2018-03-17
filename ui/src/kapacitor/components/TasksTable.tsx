import React from 'react'
import PropTypes from 'prop-types'
import {Link} from 'react-router'
import _ from 'lodash'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import {TASKS_TABLE} from 'src/kapacitor/constants/tableSizing'

const {colName, colType, colEnabled, colActions} = TASKS_TABLE

const TasksTable = ({tasks, source, onDelete, onChangeRuleStatus}) =>
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

const handleDelete = (task, onDelete) => onDelete(task)

const TaskRow = ({task, source, onDelete, onChangeRuleStatus}) =>
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
          defaultChecked={task.status === 'enabled'}
          onClick={onChangeRuleStatus(task)}
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
        confirmAction={handleDelete(task, onDelete)}
      />
    </td>
  </tr>

const {arrayOf, func, shape, string} = PropTypes

TasksTable.propTypes = {
  tasks: arrayOf(shape()),
  onChangeRuleStatus: func,
  onDelete: func,
  source: shape({
    id: string.isRequired,
  }).isRequired,
}

TaskRow.propTypes = {
  task: shape(),
  source: shape(),
  onChangeRuleStatus: func,
  onDelete: func,
}

export default TasksTable
