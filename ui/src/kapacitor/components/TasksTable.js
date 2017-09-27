import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import {TASKS_TABLE} from 'src/kapacitor/constants/tableSizing'

const {colID, colType, colEnabled, colActions} = TASKS_TABLE

const TasksTable = ({tasks, source, onDelete, onChangeRuleStatus}) =>
  <div className="panel-body">
    <table className="table v-center">
      <thead>
        <tr>
          <th style={{width: colID}}>ID</th>
          <th style={{width: colType}}>Type</th>
          <th style={{width: colEnabled}} className="text-center">
            Enabled
          </th>
          <th style={{width: colActions}} />
        </tr>
      </thead>
      <tbody>
        {_.sortBy(tasks, t => t.name.toLowerCase()).map(task => {
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
  </div>

const handleDelete = (task, onDelete) => onDelete(task)

const TaskRow = ({task, source, onDelete, onChangeRuleStatus}) =>
  <tr key={task.id}>
    <td style={{width: colID}} className="monotype">
      <i>
        {task.id}
      </i>
    </td>
    <td style={{width: colType}} className="monotype">
      {task.type}
    </td>
    <td style={{width: colEnabled}} className="monotype text-center">
      <div className="dark-checkbox">
        <input
          id={`kapacitor-enabled ${task.id}`}
          className="form-control-static"
          type="checkbox"
          defaultChecked={task.status === 'enabled'}
          onClick={onChangeRuleStatus(task)}
        />
        <label htmlFor={`kapacitor-enabled ${task.id}`} />
      </div>
    </td>
    <td style={{width: colActions}} className="text-right table-cell-nowrap">
      <Link
        className="btn btn-info btn-xs"
        to={`/sources/${source.id}/tickscript/${task.id}`}
      >
        Edit TICKscript
      </Link>
      <button
        className="btn btn-danger btn-xs"
        onClick={handleDelete(task, onDelete)}
      >
        Delete
      </button>
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
