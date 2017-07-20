import React, {PropTypes} from 'react'

import {
  DEFAULT_ALERT_PLACEHOLDERS,
  DEFAULT_ALERT_LABELS,
  ALERT_NODES_ACCESSORS,
} from '../constants'

const RuleMessageAlertConfig = ({updateAlertNodes, alert, rule}) => {
  if (!Object.keys(DEFAULT_ALERT_PLACEHOLDERS).find(a => a === alert)) {
    return null
  }
  if (!Object.keys(DEFAULT_ALERT_LABELS).find(a => a === alert)) {
    return null
  }
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>
        {DEFAULT_ALERT_LABELS[alert]}
      </p>
      <input
        id="alert-input"
        className="form-control input-sm form-malachite"
        style={{flex: '1 0 0'}}
        type="text"
        placeholder={DEFAULT_ALERT_PLACEHOLDERS[alert]}
        onChange={e => updateAlertNodes(rule.id, alert, e.target.value)}
        value={ALERT_NODES_ACCESSORS[alert](rule)}
        autoComplete="off"
        spellCheck="false"
      />
    </div>
  )
}

const {func, shape, string} = PropTypes

RuleMessageAlertConfig.propTypes = {
  updateAlertNodes: func.isRequired,
  alert: string,
  rule: shape({}).isRequired,
}

export default RuleMessageAlertConfig
