import React, {Component, PropTypes} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import uuid from 'node-uuid'

import RuleMessageOptions from 'src/kapacitor/components/RuleMessageOptions'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'
import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ALERTS, RULE_ALERT_OPTIONS} from 'src/kapacitor/constants'

const alertNodesToEndpoints = rule => {
  const endpointsOfKind = {}
  const endpointsOnThisAlert = []
  rule.alertNodes.forEach(ep => {
    const count = _.get(endpointsOfKind, ep.name, 0) + 1
    endpointsOfKind[ep.name] = count
    endpointsOnThisAlert.push({
      text: ep.name + count,
      kind: ep.name,
      ruleID: rule.id,
    })
  })
  const selectedEndpoint = endpointsOnThisAlert.length
    ? endpointsOnThisAlert[0]
    : null
  return {endpointsOnThisAlert, selectedEndpoint, endpointsOfKind}
}

class RuleMessage extends Component {
  constructor(props) {
    super(props)
    const {
      endpointsOnThisAlert,
      selectedEndpoint,
      endpointsOfKind,
    } = alertNodesToEndpoints(this.props.rule)

    this.state = {
      selectedEndpoint,
      endpointsOnThisAlert,
      endpointsOfKind,
    }
  }

  handleChangeMessage = e => {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, e.target.value)
  }

  handleChooseAlert = item => () => {
    const {actions} = this.props
    actions.updateAlerts(item.ruleID, [item.text])
    actions.updateAlertNodes(item.ruleID, item.text, '')
    this.setState({selectedEndpoint: item})
  }

  handleAddEndpoint = selectedItem => {
    const {endpointsOnThisAlert, endpointsOfKind} = this.state
    const newItemNumbering = _.get(endpointsOfKind, selectedItem.text, 0) + 1
    const newItemName = selectedItem.text + newItemNumbering
    const newEndpoint = {
      text: newItemName,
      kind: selectedItem.text,
      ruleID: selectedItem.ruleID,
    }
    this.setState({
      endpointsOnThisAlert: [...endpointsOnThisAlert, newEndpoint],
      endpointsOfKind: {
        ...endpointsOfKind,
        [selectedItem.text]: newItemNumbering,
      },
      selectedEndpoint: newEndpoint,
    })
  }

  handleRemoveEndpoint = alert => e => {
    e.stopPropagation()
    const {endpointsOnThisAlert, selectedEndpoint} = this.state
    const removedIndex = _.findIndex(endpointsOnThisAlert, ['text', alert.text])
    const remainingEndpoints = _.reject(endpointsOnThisAlert, [
      'text',
      alert.text,
    ])
    if (selectedEndpoint.text === alert.text) {
      const selectedIndex = removedIndex > 0 ? removedIndex - 1 : 0
      const newSelected = remainingEndpoints.length
        ? remainingEndpoints[selectedIndex]
        : null
      this.setState({selectedEndpoint: newSelected})
    }
    this.setState({
      endpointsOnThisAlert: remainingEndpoints,
    })
  }

  render() {
    const {rule, actions, enabledAlerts} = this.props
    const {endpointsOnThisAlert, selectedEndpoint} = this.state
    const defaultAlertEndpoints = DEFAULT_ALERTS.map(text => {
      return {text, kind: text, ruleID: rule.id}
    })
    const alerts = [
      ...defaultAlertEndpoints,
      ...enabledAlerts.map(text => {
        return {text, kind: text, ruleID: rule.id}
      }),
    ]
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
                    )
                    .map(alert =>
                      <li
                        key={uuid.v4()}
                        className={classnames({
                          active:
                            alert.text ===
                            (selectedEndpoint && selectedEndpoint.text),
                        })}
                        onClick={this.handleChooseAlert(alert)}
                      >
                        {alert.text}
                        <div
                          className="nav-tab--delete"
                          onClick={this.handleRemoveEndpoint(alert)}
                        />
                      </li>
                    )}
                </ul>
              : null}
            <Dropdown
              items={alerts}
              menuClass="dropdown-malachite"
              selected="Add an Endpoint"
              onChoose={this.handleAddEndpoint}
              className="dropdown-140 rule-message--add-endpoint"
            />
          </div>
          {endpointsOnThisAlert.length
            ? <div>
                <RuleMessageOptions
                  rule={rule}
                  alertNode={selectedEndpoint}
                  updateAlertNodes={actions.updateAlertNodes}
                  updateDetails={actions.updateDetails}
                  updateAlertProperty={actions.updateAlertProperty}
                />
                <RuleMessageText
                  rule={rule}
                  updateMessage={this.handleChangeMessage}
                  alertNodeName={selectedEndpoint}
                />
                <RuleMessageTemplates
                  rule={rule}
                  updateMessage={actions.updateMessage}
                  alertNodeName={selectedEndpoint}
                />
              </div>
            : null}
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
