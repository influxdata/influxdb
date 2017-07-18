import React, {PropTypes} from 'react'
import classnames from 'classnames'

import RuleMessageConfig from 'src/kapacitor/components/RuleMessageConfig'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'

import {RULE_MESSAGE_TEMPLATES, DEFAULT_ALERTS} from '../constants'

const {arrayOf, func, shape, string} = PropTypes

export const RuleMessage = React.createClass({
  propTypes: {
    rule: shape({}).isRequired,
    actions: shape({
      updateMessage: func.isRequired,
      updateDetails: func.isRequired,
    }).isRequired,
    enabledAlerts: arrayOf(string.isRequired).isRequired,
  },

  getInitialState() {
    return {
      selectedAlert: null,
      selectedAlertProperty: null,
    }
  },

  handleChangeMessage() {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, this.message.value)
  },

  handleChooseAlert(item) {
    const {actions} = this.props
    actions.updateAlerts(item.ruleID, [item.text])
    actions.updateAlertNodes(item.ruleID, item.text, '')
    this.setState({selectedAlert: item.text})
  },

  render() {
    const {rule, actions, enabledAlerts} = this.props
    const defaultAlertEndpoints = DEFAULT_ALERTS.map(text => {
      return {text, ruleID: rule.id}
    })

    const alerts = [
      ...defaultAlertEndpoints,
      ...enabledAlerts.map(text => {
        return {text, ruleID: rule.id}
      }),
    ]

    const selectedAlert = rule.alerts[0] || alerts[0].text

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Alert Message</h3>
        <div className="rule-section--body">
          <div className="rule-section--row rule-section--row-first rule-section--border-bottom">
            <p>Send this Alert to:</p>
            <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
              {alerts.map(alert =>
                <li
                  key={alert.text}
                  className={classnames({
                    active: alert.text === selectedAlert,
                  })}
                  onClick={() => this.handleChooseAlert(alert)}
                >
                  {alert.text}
                </li>
              )}
            </ul>
          </div>
          <RuleMessageConfig
            updateAlertNodes={actions.updateAlertNodes}
            alert={selectedAlert}
            rule={rule}
          />
          {selectedAlert === 'smtp'
            ? <div className="rule-section--border-bottom">
                <textarea
                  className="form-control form-malachite monotype rule-builder--message"
                  placeholder="Email body text goes here"
                  ref={r => (this.details = r)}
                  onChange={() =>
                    actions.updateDetails(rule.id, this.details.value)}
                  value={rule.details}
                  spellCheck={false}
                />
              </div>
            : null}
          <RuleMessageText rule={rule} actions={actions} />
          <RuleMessageTemplates
            rule={rule}
            actions={actions}
            templates={RULE_MESSAGE_TEMPLATES}
          />
        </div>
      </div>
    )
  },
})

export default RuleMessage
