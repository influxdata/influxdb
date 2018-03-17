import React, {PureComponent, SFC, MouseEvent} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import {AlertRule, Source} from 'src/types'

import ConfirmButton from 'src/shared/components/ConfirmButton'
import {parseAlertNodeList} from 'src/shared/parsing/parseHandlersFromRule'
import {TASKS_TABLE} from 'src/kapacitor/constants/tableSizing'
const {
  colName,
  colTrigger,
  colMessage,
  colAlerts,
  colEnabled,
  colActions,
} = TASKS_TABLE

interface KapacitorRulesTableProps {
  rules: AlertRule[]
  source: Source
  onChangeRuleStatus: (rule: AlertRule) => void
  onDelete: (rule: AlertRule) => void
}

interface RuleRowProps {
  rule: AlertRule
  source: Source
  onChangeRuleStatus: (rule: AlertRule) => void
  onDelete: (rule: AlertRule) => void
}

const KapacitorRulesTable: SFC<KapacitorRulesTableProps> = ({
  rules,
  source,
  onChangeRuleStatus,
  onDelete,
}) => (
  <table className="table v-center table-highlight">
    <thead>
      <tr>
        <th style={{minWidth: colName}}>Name</th>
        <th style={{width: colTrigger}}>Rule Type</th>
        <th style={{width: colMessage}}>Message</th>
        <th style={{width: colAlerts}}>Alert Handlers</th>
        <th style={{width: colEnabled}} className="text-center">
          Task Enabled
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
)

const handleDelete = (rule, onDelete) => onDelete(rule)

class RuleRow extends PureComponent<RuleRowProps> {
  handleClickRuleStatusEnabled(_: MouseEvent<HTMLInputElement>) {
    return (rule: AlertRule) => this.props.onChangeRuleStatus(rule)
  }

  render() {
    const {rule, source, onDelete} = this.props

    return (
      <tr key={rule.id}>
        <td style={{minWidth: colName}}>
          <Link to={`/sources/${source.id}/alert-rules/${rule.id}`}>
            {rule.name}
          </Link>
        </td>
        <td style={{width: colTrigger, textTransform: 'capitalize'}}>
          {rule.trigger}
        </td>
        <td style={{width: colMessage}}>{rule.message}</td>
        <td style={{width: colAlerts}}>{parseAlertNodeList(rule)}</td>
        <td style={{width: colEnabled}} className="text-center">
          <div className="dark-checkbox">
            <input
              id={`kapacitor-rule-row-task-enabled ${rule.id}`}
              className="form-control-static"
              type="checkbox"
              defaultChecked={rule.status === 'enabled'}
              onClick={this.handleClickRuleStatusEnabled}
            />
            <label htmlFor={`kapacitor-rule-row-task-enabled ${rule.id}`} />
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
    )
  }
}

export default KapacitorRulesTable
