import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const KapacitorRulesTable = ({rules, onDelete, onChangeRuleStatus}) => (
  <div className="panel-body">
    <table className="table v-center">
      <thead>
        <tr>
          <th>Name</th>
          <th>Rule Type</th>
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
              onDelete={onDelete}
              onChangeRuleStatus={onChangeRuleStatus}
            />
          )
        })}
      </tbody>
    </table>
  </div>
)

const RuleRow = ({rule, onDelete, onChangeRuleStatus}) => (
  <tr key={rule.id}>
    <td className="monotype">
      <RuleTitle rule={rule} />
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
      <button className="btn btn-danger btn-xs" onClick={() => onDelete(rule)}>
        Delete
      </button>
    </td>
  </tr>
)

const RuleTitle = ({rule: {name, links, query}}) => {
  // no queryConfig means the rule was manually created outside of Chronograf
  if (!query) {
    return <i>{name}</i>
  }

  return <Link to={links.self}>{name}</Link>
}

const {arrayOf, func, shape, string} = PropTypes

KapacitorRulesTable.propTypes = {
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

RuleTitle.propTypes = {
  rule: shape({
    name: string.isRequired,
    query: shape(),
    links: shape({
      self: string.isRequired,
    }).isRequired,
  }),
}

export default KapacitorRulesTable
