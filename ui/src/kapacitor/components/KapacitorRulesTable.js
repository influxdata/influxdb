import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const KapacitorRulesTable = ({source, rules, onDelete, onChangeRuleStatus}) => {
  return (
    <div className="panel-body">
      <table className="table v-center">
        <thead>
          <tr>
            <th>Name</th>
            <th>Trigger</th>
            <th>Message</th>
            <th>Alerts</th>
            <th className="text-center">Enabled</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {rules.map(rule => {
            return (
              <RuleRow
                key={rule.id}
                rule={rule}
                source={source}
                onDelete={onDelete}
                onChangeRuleStatus={onChangeRuleStatus}
              />
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const RuleRow = ({rule, source, onDelete, onChangeRuleStatus}) => {
  return (
    <tr key={rule.id}>
      <td className="monotype">
        <Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>
          {rule.name}
        </Link>
      </td>
      <td className="monotype">{rule.trigger}</td>
      <td className="monotype">{rule.message}</td>
      <td className="monotype">{rule.alerts.join(', ')}</td>
      <td className="monotype text-center">
        <div className="dark-checkbox">
          <input
            id={`kapacitor-enabled ${rule.id}`}
            className="form-control-static"
            type="checkbox"
            defaultChecked={rule.status === 'enabled'}
            onClick={() => onChangeRuleStatus(rule)}
          />
          <label htmlFor={`kapacitor-enabled ${rule.id}`} />
        </div>
      </td>
      <td className="text-right">
        <button
          className="btn btn-danger btn-xs"
          onClick={() => onDelete(rule)}
        >
          Delete
        </button>
      </td>
    </tr>
  )
}

const {arrayOf, func, shape} = PropTypes

KapacitorRulesTable.propTypes = {
  source: shape(),
  rules: arrayOf(shape()),
  onChangeRuleStatus: func,
  onDelete: func,
}

RuleRow.propTypes = {
  rule: shape(),
  source: shape(),
  onChangeRuleStatus: func,
  onDelete: func,
}

export default KapacitorRulesTable
