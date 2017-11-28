import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import HandlerOptions from 'src/kapacitor/components/HandlerOptions'
import HandlerTabs from 'src/kapacitor/components/HandlerTabs'
import Dropdown from 'shared/components/Dropdown'

import {DEFAULT_ALERTS} from 'src/kapacitor/constants'

const alertNodesToEndpoints = rule => {
  const handlersOfKind = {}
  const handlersOnThisAlert = []
  rule.alertNodes.forEach(an => {
    const count = _.get(handlersOfKind, an.name, 0) + 1
    handlersOfKind[an.name] = count
    const ep = {
      ...an.properties,
      ...an.args,
      ...an,
      alias: an.name + count,
      type: an.name,
    }
    handlersOnThisAlert.push(ep)
  })
  const selectedHandler = handlersOnThisAlert.length
    ? handlersOnThisAlert[0]
    : null
  return {handlersOnThisAlert, selectedHandler, handlersOfKind}
}

class RuleHandlers extends Component {
  constructor(props) {
    super(props)
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = alertNodesToEndpoints(this.props.rule)

    this.state = {
      selectedHandler,
      handlersOnThisAlert,
      handlersOfKind,
    }
  }

  handleChangeMessage = e => {
    const {actions, rule} = this.props
    actions.updateMessage(rule.id, e.target.value)
  }

  handleChooseHandler = ep => () => {
    this.setState({selectedHandler: ep})
  }

  handleAddEndpoint = selectedItem => {
    const {handlersOnThisAlert, handlersOfKind} = this.state
    const newItemNumbering = _.get(handlersOfKind, selectedItem.type, 0) + 1
    const newItemName = `${selectedItem.type}-${newItemNumbering}`
    const newEndpoint = {
      ...selectedItem,
      alias: newItemName,
    }
    this.setState(
      {
        handlersOnThisAlert: [...handlersOnThisAlert, newEndpoint],
        handlersOfKind: {
          ...handlersOfKind,
          [selectedItem.type]: newItemNumbering,
        },
        selectedHandler: newEndpoint,
      },
      this.handleUpdateAllAlerts
    )
  }

  handleRemoveHandler = removedEP => e => {
    e.stopPropagation()
    const {handlersOnThisAlert, selectedHandler} = this.state
    const removedIndex = _.findIndex(handlersOnThisAlert, [
      'alias',
      removedEP.alias,
    ])
    const remainingEndpoints = _.reject(handlersOnThisAlert, [
      'alias',
      removedEP.alias,
    ])
    if (selectedHandler.alias === removedEP.alias) {
      const selectedIndex = removedIndex > 0 ? removedIndex - 1 : 0
      const newSelected = remainingEndpoints.length
        ? remainingEndpoints[selectedIndex]
        : null
      this.setState({selectedHandler: newSelected})
    }
    this.setState(
      {handlersOnThisAlert: remainingEndpoints},
      this.handleUpdateAllAlerts
    )
  }

  handleUpdateAllAlerts = () => {
    const {rule, actions} = this.props
    const {handlersOnThisAlert} = this.state

    actions.updateAlertNodes(rule.id, handlersOnThisAlert)
  }

  handleModifyHandler = (selectedHandler, fieldName) => e => {
    const {handlersOnThisAlert} = this.state
    const modifiedEP =
      e.target.type === 'checkbox'
        ? {
            ...selectedHandler,
            [fieldName]: !selectedHandler[fieldName],
          }
        : {
            ...selectedHandler,
            [fieldName]: e.target.value,
          }
    const remainingEndpoints = _.reject(handlersOnThisAlert, [
      'alias',
      modifiedEP.alias,
    ])
    this.setState(
      {
        selectedHandler: modifiedEP,
        handlersOnThisAlert: [...remainingEndpoints, modifiedEP],
      },
      this.handleUpdateAllAlerts
    )
  }

  render() {
    const {handlersFromConfig, configLink} = this.props
    const {handlersOnThisAlert, selectedHandler} = this.state
    const alerts = _.map([...DEFAULT_ALERTS, ...handlersFromConfig], a => {
      return {...a, text: a.type}
    })
    const dropdownLabel = handlersOnThisAlert.length
      ? 'Add another Handler'
      : 'Add a Handler'

    const ruleSectionClassName = handlersOnThisAlert.length
      ? 'rule-section--row rule-section--row-first rule-section--border-bottom'
      : 'rule-section--row rule-section--row-first rule-section--row-last'

    return (
      <div className="rule-section">
        <h3 className="rule-section--heading">Alert Handlers</h3>
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
          {handlersOnThisAlert.length
            ? <div className="rule-message--endpoints">
                <HandlerTabs
                  handlersOnThisAlert={handlersOnThisAlert}
                  selectedHandler={selectedHandler}
                  handleChooseHandler={this.handleChooseHandler}
                  handleRemoveHandler={this.handleRemoveHandler}
                />
                <HandlerOptions
                  configLink={configLink}
                  selectedHandler={selectedHandler}
                  handleModifyHandler={this.handleModifyHandler}
                />
              </div>
            : null}
        </div>
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

RuleHandlers.propTypes = {
  rule: shape({}).isRequired,
  actions: shape({
    updateAlertNodes: func.isRequired,
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
    updateAlertProperty: func.isRequired,
  }).isRequired,
  handlersFromConfig: arrayOf(shape({})),
  configLink: string,
}

export default RuleHandlers
