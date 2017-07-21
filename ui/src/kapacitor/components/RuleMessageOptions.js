import React, {Component, PropTypes} from 'react'

import {
  RULE_ALERT_OPTIONS,
  ALERT_NODES_ACCESSORS,
} from 'src/kapacitor/constants'

class RuleMessageOptions extends Component {
  constructor(props) {
    super(props)

    this.getAlertPropertyValue = ::this.getAlertPropertyValue
  }

  getAlertPropertyValue(properties, name) {
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

  render() {
    const {
      rule,
      alertNodeName,
      updateAlertNodes,
      updateDetails,
      updateAlertProperty,
    } = this.props
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
                style={{flex: '1 0 0'}}
                type="text"
                placeholder={args.placeholder}
                onChange={e =>
                  updateAlertNodes(rule.id, alertNodeName, e.target.value)}
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
                          flex: '1 0 0',
                        }}
                        type="text"
                        placeholder={placeholder}
                        onChange={e =>
                          updateAlertProperty(rule.id, alertNodeName, {
                            name: propertyName,
                            args: [e.target.value],
                          })}
                        value={this.getAlertPropertyValue(
                          rule.alertNodes[0].properties,
                          propertyName
                        )}
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
                ref={r => (this.details = r)}
                onChange={() => updateDetails(rule.id, this.details.value)}
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
