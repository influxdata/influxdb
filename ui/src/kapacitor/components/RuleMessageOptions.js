import React, {Component, PropTypes} from 'react'

import {
  RULE_ALERT_OPTIONS,
  ALERT_NODES_ACCESSORS,
} from 'src/kapacitor/constants'

class RuleMessageOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {rule, alert, updateAlertNodes, updateDetails} = this.props
    const {args, details, properties} = RULE_ALERT_OPTIONS[alert]

    return (
      <div>
        {args
          ? <div className="rule-section--row rule-section--border-bottom">
              <p>{args.label}</p>
              <input
                id="alert-input"
                className="form-control input-sm form-malachite"
                style={{flex: '1 0 0'}}
                type="text"
                placeholder={args.placeholder}
                onChange={e => updateAlertNodes(rule.id, alert, e.target.value)}
                value={ALERT_NODES_ACCESSORS[alert](rule)}
                autoComplete="off"
                spellCheck="false"
              />
            </div>
          : null}
        {details
          ? <div className="rule-section--border-bottom">
              <textarea
                className="form-control form-malachite monotype rule-builder--message"
                placeholder={details.placeholder ? details.placeholder : ''}
                ref={r => (this.details = r)}
                onChange={() => updateDetails(rule.id, this.details.value)}
                value={rule.details}
                spellCheck={false}
              />
            </div>
          : null}
        {properties
          ? properties.map(({key, label, placeholder}) =>
              <div
                key={key}
                className="rule-section--row rule-section--border-bottom"
              >
                <p>{label}</p>
                <input
                  className="form-control input-sm form-malachite"
                  style={{flex: '1 0 0'}}
                  type="text"
                  placeholder={placeholder}
                  // onChange={e => updateProperties(rule.id, e.target.value)}
                  value={ALERT_NODES_ACCESSORS[alert](rule)}
                  autoComplete="off"
                  spellCheck="false"
                />
              </div>
            )
          : null}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

RuleMessageOptions.propTypes = {
  rule: shape({}).isRequired,
  alert: string,
  updateAlertNodes: func.isRequired,
  updateDetails: func.isRequired,
}

export default RuleMessageOptions
