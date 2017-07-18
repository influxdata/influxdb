import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import RuleMessageOptions from 'src/kapacitor/components/RuleMessageOptions'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'

import {DEFAULT_ALERTS} from 'src/kapacitor/constants'

class RuleMessage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      selectedAlert: null,
      selectedAlertProperty: null,
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
    this.setState({selectedAlert: item.text})
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
          <RuleMessageOptions
            rule={rule}
            alert={selectedAlert}
            updateAlertNodes={actions.updateAlertNodes}
            updateDetails={actions.updateDetails}
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
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
  }).isRequired,
  enabledAlerts: arrayOf(string.isRequired).isRequired,
}

export default RuleMessage
