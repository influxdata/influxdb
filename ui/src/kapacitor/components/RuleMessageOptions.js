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
    const {updateAlertNodes, alertNodeName, rule} = this.props
    updateAlertNodes(rule.id, alertNodeName, e.target.value)
  }

  handleUpdateAlertProperty = propertyName => e => {
    const {updateAlertProperty, alertNodeName, rule} = this.props
    updateAlertProperty(rule.id, alertNodeName, {
      name: propertyName,
      args: [e.target.value],
    })
  }

  render() {
    const {rule, alertNodeName} = this.props
    const {args, details, properties} = RULE_ALERT_OPTIONS[alertNodeName]

    return (
      <div>
        {args
          ? <div className="rule-section--row rule-section--border-bottom">
              <p>
                {args.label}
              </p>
              <input
                id="alert-input"
                className="form-control input-sm form-malachite"
                style={{flex: '1 0 0%'}}
                type="text"
                placeholder={args.placeholder}
                onChange={this.handleUpdateAlertNodes}
                value={ALERT_NODES_ACCESSORS[alertNodeName](rule)}
                autoComplete="off"
                spellCheck="false"
              />
            </div>
          : null}
        {properties && properties.length
          ? <div
              className="rule-section--row rule-section--border-bottom"
              style={{display: 'block'}}
            >
              <p>Optional Alert Parameters</p>
              <div style={{display: 'flex', flexWrap: 'wrap'}}>
                {properties.map(({name: propertyName, label, placeholder}) =>
                  <div
                    key={propertyName}
                    style={{display: 'block', flex: '0 0 33.33%'}}
                  >
                    <label
                      htmlFor={label}
                      style={{
                        display: 'flex',
                        width: '100%',
                        alignItems: 'center',
                      }}
                    >
                      <span style={{flex: '0 0 auto'}}>
                        {label}
                      </span>
                      <input
                        name={label}
                        className="form-control input-sm form-malachite"
                        style={{
                          margin: '0 15px 0 5px',
                          flex: '1 0 0%',
                        }}
                        type="text"
                        placeholder={placeholder}
                        onChange={this.handleUpdateAlertProperty(propertyName)}
                        value={this.getAlertPropertyValue(propertyName)}
                        autoComplete="off"
                        spellCheck="false"
                      />
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

const {func, shape, string} = PropTypes

RuleMessageOptions.propTypes = {
  rule: shape({}).isRequired,
  alertNodeName: string,
  updateAlertNodes: func.isRequired,
  updateDetails: func.isRequired,
  updateAlertProperty: func.isRequired,
}

export default RuleMessageOptions
