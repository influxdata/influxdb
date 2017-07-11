import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const KapacitorRulesTable = ({
  rules,
  source,
  onDelete,
  onReadTickscript,
  onChangeRuleStatus,
}) => (
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
              source={source}
              onDelete={onDelete}
              onChangeRuleStatus={onChangeRuleStatus}
              onRead={onReadTickscript}
            />
          )
        })}
      </tbody>
    </table>
  </div>
)

const RuleRow = ({rule, source, onRead, onDelete, onChangeRuleStatus}) => (
  <tr key={rule.id}>
    <td className="monotype">
      <RuleTitle rule={rule} source={source} />
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
    <td className="text-right" style={{display: 'flex'}}>
      <button className="btn btn-info btn-xs" onClick={() => onRead(rule)}>
        View TICKscript
      </button>
      <button className="btn btn-danger btn-xs" onClick={() => onDelete(rule)}>
        Delete
      </button>
    </td>
  </tr>
)

const RuleTitle = ({rule: {id, name, query}, source}) => {
  // no queryConfig means the rule was manually created outside of Chronograf
  if (!query) {
    return <i>{name}</i>
  }

  return <Link to={`/sources/${source.id}/alert-rules/${id}`}>{name}</Link>
}

const {arrayOf, func, shape, string} = PropTypes

KapacitorRulesTable.propTypes = {
  rules: arrayOf(shape()),
  onChangeRuleStatus: func,
  onDelete: func,
  source: shape({
    id: string.isRequired,
  }).isRequired,
  onReadTickscript: func,
}

RuleRow.propTypes = {
  rule: shape(),
  source: shape(),
  onChangeRuleStatus: func,
  onDelete: func,
  onRead: func,
}

RuleTitle.propTypes = {
  rule: shape({
    name: string.isRequired,
    query: shape(),
    links: shape({
      self: string.isRequired,
    }),
  }),
  source: shape({
    id: string.isRequired,
  }).isRequired,
}

export default KapacitorRulesTable
