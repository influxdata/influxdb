import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import {parseAlertNodeList} from 'src/shared/parsing/parseHandlersFromRule'
import {KAPACITOR_RULES_TABLE} from 'src/kapacitor/constants/tableSizing'
const {
  colName,
  colTrigger,
  colMessage,
  colAlerts,
  colEnabled,
  colActions,
} = KAPACITOR_RULES_TABLE

const KapacitorRulesTable = ({rules, source, onDelete, onChangeRuleStatus}) =>
  <div className="panel-body">
    <table className="table v-center table-highlight">
      <thead>
        <tr>
          <th style={{minWidth: colName}}>Name</th>
          <th style={{width: colTrigger}}>Rule Trigger</th>
          <th style={{width: colMessage}}>Message</th>
          <th style={{width: colAlerts}}>Alerts</th>
          <th style={{width: colEnabled}} className="text-center">
            Enabled
          </th>
          <th style={{width: colActions}} />
        </tr>
      </thead>
      <tbody>
        {_.sortBy(rules, r => r.name.toLowerCase()).map(rule => {
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

const handleDelete = (rule, onDelete) => onDelete(rule)

const RuleRow = ({rule, source, onDelete, onChangeRuleStatus}) =>
  <tr key={rule.id}>
    <td style={{minWidth: colName}}>
      <Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>
        {rule.name}
      </Link>
    </td>
    <td style={{width: colTrigger, textTransform: 'capitalize'}}>
      {rule.trigger}
    </td>
    <td style={{wdith: colMessage}}>
      {rule.message}
    </td>
    <td style={{width: colAlerts}}>
      {parseAlertNodeList(rule)}
    </td>
    <td style={{width: colEnabled}} className="text-center">
      <div className="dark-checkbox">
        <input
          id={`kapacitor-enabled ${rule.id}`}
          className="form-control-static"
          type="checkbox"
          defaultChecked={rule.status === 'enabled'}
          onClick={onChangeRuleStatus(rule)}
        />
        <label htmlFor={`kapacitor-enabled ${rule.id}`} />
      </div>
    </td>
    <td style={{width: colActions}} className="text-right">
      <ConfirmButton
        text="Delete"
        type="btn-danger"
        size="btn-xs"
        customClass="table--show-on-row-hover"
        confirmAction={handleDelete(rule, onDelete)}
      />
    </td>
  </tr>

const {arrayOf, func, shape, string} = PropTypes

KapacitorRulesTable.propTypes = {
  rules: arrayOf(shape()),
  onChangeRuleStatus: func,
  onDelete: func,
  source: shape({
    id: string.isRequired,
  }).isRequired,
}

RuleRow.propTypes = {
  rule: shape(),
  source: shape(),
  onChangeRuleStatus: func,
  onDelete: func,
}

export default KapacitorRulesTable
