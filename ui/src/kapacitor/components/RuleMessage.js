import React, {Component, PropTypes} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import uuid from 'node-uuid'

import RuleMessageOptions from 'src/kapacitor/components/RuleMessageOptions'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'
import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ALERTS, RULE_ALERT_OPTIONS} from 'src/kapacitor/constants'

class RuleMessage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      selectedAlertNodeName: null,
      endpointsOnThisAlert: [],
<<<<<<< HEAD
      endpointsOfKind: {},
=======
>>>>>>> Add functionality to Dropdown for new alert addition
    }
  }

  handleChangeMessage = e => {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, e.target.value)
  }

  handleChooseAlert = item => () => {
    const {actions} = this.props
    actions.updateAlerts(item.ruleID, [item.text]) // TODO: this seems to be doing a lot more than it needs to.
    actions.updateAlertNodes(item.ruleID, item.text, '')
    this.setState({selectedAlertNodeName: item.text})
  }

  handleAddNewAlertEndpoint = selectedItem => {
<<<<<<< HEAD
    const {endpointsOnThisAlert, endpointsOfKind} = this.state
    const newItemNumbering = _.get(endpointsOfKind, selectedItem.text, 0) + 1
    const newItemName = selectedItem.text + newItemNumbering
    this.setState({
      endpointsOnThisAlert: _.concat(endpointsOnThisAlert, {
        text: newItemName,
        kind: selectedItem.text,
        ruleID: selectedItem.ruleID,
      }),
      endpointsOfKind: {
        ...endpointsOfKind,
        [selectedItem.text]: newItemNumbering,
      },
    })
    console.log(this.state.endpointsOnThisAlert)
  }
  handleRemoveAlertEndpoint = removedItem => {
    console.log(removedItem)
=======
    this.setState({
      endpointsOnThisAlert: _.concat(
        this.state.endpointsOnThisAlert,
        selectedItem
      ),
    })
>>>>>>> Add functionality to Dropdown for new alert addition
  }

  render() {
    const {rule, actions, enabledAlerts} = this.props
    const {endpointsOnThisAlert} = this.state
    const defaultAlertEndpoints = DEFAULT_ALERTS.map(text => {
      return {text, kind: text, ruleID: rule.id}
    })

    const alerts = [
      ...defaultAlertEndpoints,
      ...enabledAlerts.map(text => {
        return {text, kind: text, ruleID: rule.id}
      }),
    ]

    const selectedAlertNodeName = rule.alerts[0] || alerts[0].text

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Alert Message</h3>
        <div className="rule-section--body">
          <div className="rule-section--row rule-section--row-first rule-section--border-bottom">
            <p>Send this Alert to:</p>
            {endpointsOnThisAlert.length
              ? <ul className="nav nav-tablist nav-tablist-sm nav-tablist-malachite">
                  {endpointsOnThisAlert
                    .filter(alert =>
                      _.get(RULE_ALERT_OPTIONS, alert.kind, false)
                    ) // / TODO this looks like a problem
                    .map(alert =>
                      <li
                        key={uuid.v4()}
                        className={classnames({
                          active: alert.text === selectedAlertNodeName,
                        })}
                        onClick={this.handleChooseAlert(alert)}
                      >
                        {alert.text}
                        <div className="nav-tab--delete" />
                      </li>
                    )}
                </ul>
              : null}
            <Dropdown
              items={alerts}
              menuClass="dropdown-malachite"
              selected="Add an Endpoint"
              onChoose={this.handleAddNewAlertEndpoint}
              className="dropdown-140 rule-message--add-endpoint"
            />
          </div>
          <RuleMessageOptions
            rule={rule}
            alertNodeName={selectedAlertNodeName}
            updateAlertNodes={actions.updateAlertNodes}
            updateDetails={actions.updateDetails}
            updateAlertProperty={actions.updateAlertProperty}
          />
          <RuleMessageText
            rule={rule}
            updateMessage={this.handleChangeMessage}
          />
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
