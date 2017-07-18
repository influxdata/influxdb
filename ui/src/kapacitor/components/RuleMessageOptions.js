import React, {Component, PropTypes} from 'react'

import {
  DEFAULT_ALERT_PLACEHOLDERS,
  DEFAULT_ALERT_LABELS,
  ALERT_NODES_ACCESSORS,
} from 'src/kapacitor/constants'

const RuleMessageOptions = ({rule, alert, updateAlertNodes, updateDetails}) => {
  const isDetailsTypeDefault =
    Object.keys(DEFAULT_ALERT_PLACEHOLDERS).find(a => a === alert) &&
    Object.keys(DEFAULT_ALERT_LABELS).find(a => a === alert)

  return isDetailsTypeDefault
    ? <DefaultOptions
        rule={rule}
        alert={alert}
        updateAlertNodes={updateAlertNodes}
      />
    : <NonDefaultOptions
        rule={rule}
        alert={alert}
        updateDetails={updateDetails}
      />
}

const {func, shape, string} = PropTypes

RuleMessageOptions.propTypes = {
  rule: shape({}).isRequired,
  alert: string,
  updateAlertNodes: func.isRequired,
  updateDetails: func.isRequired,
}

class DefaultOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, alert, updateAlertNodes} = this.props
    return (
      <div className="rule-section--row rule-section--border-bottom">
        <p>{DEFAULT_ALERT_LABELS[alert]}</p>
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
}

DefaultOptions.propTypes = {
  rule: shape({}).isRequired,
  alert: string,
  updateAlertNodes: func.isRequired,
}

class NonDefaultOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, alert, updateDetails} = this.props

    switch (alert) {
      case 'smtp':
        return (
          <div className="rule-section--border-bottom">
            <textarea
              className="form-control form-malachite monotype rule-builder--message"
              placeholder="Email body text goes here"
              ref={r => (this.details = r)}
              onChange={() => updateDetails(rule.id, this.details.value)}
              value={rule.details}
              spellCheck={false}
            />
          </div>
        )
      default:
        return null
    }
  }
}

NonDefaultOptions.propTypes = {
  rule: shape({}).isRequired,
  alert: string,
  updateDetails: func.isRequired,
}

export default RuleMessageOptions
