import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import RuleMessageOptions from 'src/kapacitor/components/RuleMessageOptions'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'

import {DEFAULT_ALERTS, RULE_ALERT_OPTIONS} from 'src/kapacitor/constants'

class RuleMessage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      selectedAlertType: null,
    }

    this.handleChangeMessage = ::this.handleChangeMessage
    this.handleChooseAlert = ::this.handleChooseAlert
  }

  handleChangeMessage() {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, this.message.value)
  }

  handleChooseAlert(item) {
    const {actions} = this.props
    actions.updateAlerts(item.ruleID, [item.text])
    actions.updateAlertNodes(item.ruleID, item.text, '')
    this.setState({selectedAlertType: item.text})
  }

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

    const selectedAlertType = rule.alerts[0] || alerts[0].text

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Alert Message</h3>
        <div className="rule-section--body">
          <div className="rule-section--row rule-section--row-first rule-section--border-bottom">
            <p>Send this Alert to:</p>
            <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
              {alerts
                // only display alert endpoints that have rule alert options configured
                .filter(alert =>
                  Object.keys(RULE_ALERT_OPTIONS).includes(alert.text)
                )
                .map(alert =>
                  <li
                    key={alert.text}
                    className={classnames({
                      active: alert.text === selectedAlertType,
                    })}
                    onClick={() => this.handleChooseAlert(alert)}
                  >
                    {alert.text}
                  </li>
                )}
            </ul>
          </div>
          <RuleMessageOptions
            rule={rule}
            alertType={selectedAlertType}
            updateAlertNodes={actions.updateAlertNodes}
            updateDetails={actions.updateDetails}
            updateAlertProperty={actions.updateAlertProperty}
          />
          <RuleMessageText rule={rule} updateMessage={actions.updateMessage} />
          <RuleMessageTemplates
            rule={rule}
            updateMessage={actions.updateMessage}
          />
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

RuleMessage.propTypes = {
  rule: shape({}).isRequired,
  actions: shape({
    updateAlertNodes: func.isRequired,
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
    updateAlertProperty: func.isRequired,
  }).isRequired,
  enabledAlerts: arrayOf(string.isRequired).isRequired,
}

export default RuleMessage
