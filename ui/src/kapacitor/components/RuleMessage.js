import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import RuleMessageOptions from 'src/kapacitor/components/RuleMessageOptions'
import RuleMessageText from 'src/kapacitor/components/RuleMessageText'
import RuleMessageTemplates from 'src/kapacitor/components/RuleMessageTemplates'
import EndpointTabs from 'src/kapacitor/components/EndpointTabs'
import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ALERTS} from 'src/kapacitor/constants'

const alertNodesToEndpoints = rule => {
  const endpointsOfKind = {}
  const endpointsOnThisAlert = []
  rule.alertNodes.forEach(ep => {
    const count = _.get(endpointsOfKind, ep.name, 0) + 1
    endpointsOfKind[ep.name] = count
    endpointsOnThisAlert.push({
      alias: ep.name + count,
      type: ep.name,
      args: ep.args, // TODO args+properties= options?
      properties: ep.properties,
      options: {},
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

  handleChooseAlert = ep => () => {
    this.setState({selectedEndpoint: ep})
  }

  handleAddEndpoint = selectedItem => {
    const {endpointsOnThisAlert, endpointsOfKind} = this.state
    const newItemNumbering = _.get(endpointsOfKind, selectedItem.type, 0) + 1
    const newItemName = selectedItem.type + newItemNumbering
    const newEndpoint = {
      ...selectedItem,
      alias: newItemName,
    }
    this.setState(
      {
        endpointsOnThisAlert: [...endpointsOnThisAlert, newEndpoint],
        endpointsOfKind: {
          ...endpointsOfKind,
          [selectedItem.type]: newItemNumbering,
        },
        selectedEndpoint: newEndpoint,
      },
      this.handleUpdateAllAlerts
    )
  }
  handleRemoveEndpoint = removedEP => e => {
    e.stopPropagation()
    const {endpointsOnThisAlert, selectedEndpoint} = this.state
    const removedIndex = _.findIndex(endpointsOnThisAlert, [
      'alias',
      removedEP.alias,
    ])
    const remainingEndpoints = _.reject(endpointsOnThisAlert, [
      'alias',
      removedEP.alias,
    ])
    if (selectedEndpoint.alias === removedEP.alias) {
      const selectedIndex = removedIndex > 0 ? removedIndex - 1 : 0
      const newSelected = remainingEndpoints.length
        ? remainingEndpoints[selectedIndex]
        : null
      this.setState({selectedEndpoint: newSelected})
    }
    this.setState(
      {endpointsOnThisAlert: remainingEndpoints},
      this.handleUpdateAllAlerts
    )
  }

  handleUpdateAllAlerts = () => {
    const {rule, actions} = this.props
    const {endpointsOnThisAlert} = this.state
    actions.updateAlertNodes(rule.id, endpointsOnThisAlert)
    actions.updateAlerts(rule.id, endpointsOnThisAlert)
  }

  render() {
    const {rule, actions, enabledAlerts} = this.props
    const {endpointsOnThisAlert, selectedEndpoint} = this.state
    const alerts = _.map([...DEFAULT_ALERTS, ...enabledAlerts], a => {
      return {...a, text: a.type}
    })
    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Alert Message</h3>
        <div className="rule-section--body">
          <div className="rule-section--row rule-section--row-first rule-section--border-bottom">
            <p>Send this Alert to:</p>
            <EndpointTabs
              endpointsOnThisAlert={endpointsOnThisAlert}
              selectedEndpoint={selectedEndpoint}
              handleChooseAlert={this.handleChooseAlert}
              handleRemoveEndpoint={this.handleRemoveEndpoint}
            />
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
                  selectedEndpoint={selectedEndpoint}
                  updateAlertNodes={actions.updateAlertNodes}
                  updateDetails={actions.updateDetails}
                  updateAlertProperty={actions.updateAlertProperty}
                  handleEditAlert={this.handleEditAlert}
                  handleUpdateArg={this.handleUpdateArg}
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

const {arrayOf, func, shape} = PropTypes

RuleMessage.propTypes = {
  rule: shape({}).isRequired,
  actions: shape({
    updateAlertNodes: func.isRequired,
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
    updateAlertProperty: func.isRequired,
  }).isRequired,
  enabledAlerts: arrayOf(shape({})),
}

export default RuleMessage
