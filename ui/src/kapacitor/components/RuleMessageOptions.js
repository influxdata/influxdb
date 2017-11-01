import React, {Component, PropTypes} from 'react'

import {
  RULE_ALERT_OPTIONS,
  ALERT_NODES_ACCESSORS,
} from 'src/kapacitor/constants'

class RuleMessageOptions extends Component {
  constructor(props) {
    super(props)
  }

  getAlertPropertyValue = name => {
    const {rule} = this.props
    const {properties} = rule.alertNodes[0]

    if (properties) {
      const alertNodeProperty = properties.find(
        property => property.name === name
      )
      if (alertNodeProperty) {
        return alertNodeProperty.args
      }
    }
    return ''
  }

  handleUpdateDetails = e => {
    const {updateDetails, rule} = this.props
    updateDetails(rule.id, e.target.value)
  }

  handleUpdateAlertNodes = e => {
    const {updateAlertNodes, alertNode, rule} = this.props
    updateAlertNodes(rule.id, alertNode, e.target.value)
  }

  handleUpdateAlertProperty = propertyName => e => {
    const {updateAlertProperty, alertNode, rule} = this.props
    updateAlertProperty(rule.id, alertNode, {
      name: propertyName,
      args: [e.target.value],
    })
  }

  render() {
    const {rule, alertNode} = this.props
    const {args, details, properties} = RULE_ALERT_OPTIONS[alertNode.kind]

    return (
      <div>
        {args
          ? <div className="rule-section--row rule-section--border-bottom">
              <p>Optional Alert Parameters:</p>
              <div className="optional-alert-parameters">
                <div className="form-group">
                  <input
                    name={args.label}
                    id="alert-input"
                    className="form-control input-sm form-malachite"
                    type="text"
                    placeholder={args.placeholder}
                    onChange={this.handleUpdateAlertNodes}
                    value={ALERT_NODES_ACCESSORS[alertNode.kind](rule)}
                    autoComplete="off"
                    spellCheck="false"
                  />
                  <label htmlFor={args.label}>
                    {args.label}
                  </label>
                </div>
              </div>
            </div>
          : null}
        {properties && properties.length
          ? <div className="rule-section--row rule-section--border-bottom">
              <p>Optional Alert Parameters:</p>
              <div className="optional-alert-parameters">
                {properties.map(({name: propertyName, label, placeholder}) =>
                  <div key={propertyName} className="form-group">
                    <input
                      name={label}
                      className="form-control input-sm form-malachite"
                      type="text"
                      placeholder={placeholder}
                      onChange={this.handleUpdateAlertProperty(propertyName)}
                      value={this.getAlertPropertyValue(propertyName)}
                      autoComplete="off"
                      spellCheck="false"
                    />
                    <label htmlFor={label}>
                      {label}
                    </label>
                  </div>
                )}
              </div>
            </div>
          : null}
        {details
          ? <div className="rule-section--border-bottom">
              <textarea
                className="form-control form-malachite monotype rule-builder--message"
                placeholder={details.placeholder ? details.placeholder : ''}
                onChange={this.handleUpdateDetails}
                value={rule.details}
                spellCheck={false}
              />
            </div>
          : null}
      </div>
    )
  }
}

const {func, shape} = PropTypes

RuleMessageOptions.propTypes = {
  rule: shape({}).isRequired,
  alertNode: shape({}),
  updateAlertNodes: func.isRequired,
  updateDetails: func.isRequired,
  updateAlertProperty: func.isRequired,
}

export default RuleMessageOptions
