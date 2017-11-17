import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import EndpointOptions from 'src/kapacitor/components/EndpointOptions'
import EndpointTabs from 'src/kapacitor/components/EndpointTabs'
import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ALERTS} from 'src/kapacitor/constants'

const alertNodesToEndpoints = rule => {
  const endpointsOfKind = {} // TODO why are these consts?
  const endpointsOnThisAlert = []
  rule.alertNodes.forEach(an => {
    const count = _.get(endpointsOfKind, an.name, 0) + 1
    endpointsOfKind[an.name] = count
    const ep = {
      ...an.properties,
      ...an.args,
      ...an,
      alias: an.name + count,
      type: an.name,
    }
    endpointsOnThisAlert.push(ep)
  })
  const selectedEndpoint = endpointsOnThisAlert.length
    ? endpointsOnThisAlert[0]
    : null
  return {endpointsOnThisAlert, selectedEndpoint, endpointsOfKind}
}

class RuleEndpoints extends Component {
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
  }

  handleModifyEndpoint = (selectedEndpoint, fieldName) => e => {
    const {endpointsOnThisAlert} = this.state
    const modifiedEP =
      e.target.type === 'checkbox'
        ? {
            ...selectedEndpoint,
            [fieldName]: !selectedEndpoint[fieldName],
          }
        : {
            ...selectedEndpoint,
            [fieldName]: e.target.value,
          }
    const remainingEndpoints = _.reject(endpointsOnThisAlert, [
      'alias',
      modifiedEP.alias,
    ])
    this.setState(
      {
        selectedEndpoint: modifiedEP,
        endpointsOnThisAlert: [...remainingEndpoints, modifiedEP],
      },
      this.handleUpdateAllAlerts
    )
  }

  render() {
    const {enabledAlerts} = this.props
    const {endpointsOnThisAlert, selectedEndpoint} = this.state
    const alerts = _.map([...DEFAULT_ALERTS, ...enabledAlerts], a => {
      return {...a, text: a.type}
    })

    const dropdownLabel = endpointsOnThisAlert.length
      ? 'Add another Endpoint'
      : 'Add an Endpoint'

    const ruleSectionClassName = endpointsOnThisAlert.length
      ? 'rule-section--row rule-section--row-first rule-section--border-bottom'
      : 'rule-section--row rule-section--row-first rule-section--row-last'

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Endpoints</h3>
        <div className="rule-section--body">
          <div className={ruleSectionClassName}>
            <p>Send this Alert to:</p>
            <Dropdown
              items={alerts}
              menuClass="dropdown-malachite"
              selected={dropdownLabel}
              onChoose={this.handleAddEndpoint}
              className="dropdown-200 rule-message--add-endpoint"
            />
          </div>
          {endpointsOnThisAlert.length
            ? <div className="rule-message--endpoints">
                <EndpointTabs
                  endpointsOnThisAlert={endpointsOnThisAlert}
                  selectedEndpoint={selectedEndpoint}
                  handleChooseAlert={this.handleChooseAlert}
                  handleRemoveEndpoint={this.handleRemoveEndpoint}
                />
                <EndpointOptions
                  selectedEndpoint={selectedEndpoint}
                  handleModifyEndpoint={this.handleModifyEndpoint}
                />
              </div>
            : null}
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape} = PropTypes

RuleEndpoints.propTypes = {
  rule: shape({}).isRequired,
  actions: shape({
    updateAlertNodes: func.isRequired,
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
    updateAlertProperty: func.isRequired,
  }).isRequired,
  enabledAlerts: arrayOf(shape({})),
}

export default RuleEndpoints
