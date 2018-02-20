import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import {TASKS_TABLE} from 'src/kapacitor/constants/tableSizing'

const {colID, colType, colEnabled, colActions} = TASKS_TABLE

const checkForName = task => {
  return task.id.slice(0, 10) === 'chronograf' ? task.name : task.id
}

const TasksTable = ({tasks, source, onDelete, onChangeRuleStatus}) =>
  <div className="panel-body">
    <table className="table v-center table-highlight">
      <thead>
        <tr>
          <th style={{minWidth: colID}}>ID</th>
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
    <td style={{minWidth: colID}}>
      <Link
        className="link-success"
        to={`/sources/${source.id}/tickscript/${task.id}`}
      >
        {checkForName(task)}
      </Link>
    </td>
    <td style={{width: colType, textTransform: 'capitalize'}}>
      {task.type}
    </td>
    <td style={{width: colEnabled}} className="text-center">
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
