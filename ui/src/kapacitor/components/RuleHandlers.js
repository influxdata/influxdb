import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import HandlerOptions from 'src/kapacitor/components/HandlerOptions'
import HandlerTabs from 'src/kapacitor/components/HandlerTabs'
import Dropdown from 'shared/components/Dropdown'
import {getHandlersFromRule} from 'src/shared/parsing/parseHandlersFromRule'

import {DEFAULT_HANDLERS} from 'src/kapacitor/constants'

class RuleHandlers extends Component {
  constructor(props) {
    super(props)
    const {handlersFromConfig} = this.props
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = getHandlersFromRule(this.props.rule, handlersFromConfig) // nagging feeling that this should not be in constructor. ?

    this.state = {
      selectedHandler,
      handlersOnThisAlert,
      handlersOfKind,
    }
  }

  handleChangeMessage = e => {
    const {ruleActions, rule} = this.props
    ruleActions.updateMessage(rule.id, e.target.value)
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
    const {rule, ruleActions} = this.props
    const {handlersOnThisAlert} = this.state

    ruleActions.updateAlertNodes(rule.id, handlersOnThisAlert)
  }

  handleModifyHandler = (
    selectedHandler,
    fieldName,
    parseToArray,
    headerIndex
  ) => e => {
    const {handlersOnThisAlert} = this.state
    let modifiedEP
    if (fieldName === 'headerKey') {
      const currentHeader = selectedHandler.headers || [[]]
      currentHeader[headerIndex][0] = e.target.value // only works for headerIndex= 0 atm. would need to initialize if headerindex is larger.
      modifiedEP = {
        ...selectedHandler,
        headers: [...currentHeader],
      }
    } else if (fieldName === 'headerValue') {
      const currentHeader = selectedHandler.headers || [[]]
      currentHeader[headerIndex][1] = e.target.value
      modifiedEP = {
        ...selectedHandler,
        headers: [...currentHeader],
      }
    } else if (e.target.type === 'checkbox') {
      modifiedEP = {
        ...selectedHandler,
        [fieldName]: !selectedHandler[fieldName],
      }
    } else if (parseToArray) {
      modifiedEP = {
        ...selectedHandler,
        [fieldName]: _.split(e.target.value, ' '),
      }
    } else {
      modifiedEP = {
        ...selectedHandler,
        [fieldName]: e.target.value,
      }
    }

    const modifiedIndex = _.findIndex(handlersOnThisAlert, [
      'alias',
      modifiedEP.alias,
    ])

    handlersOnThisAlert[modifiedIndex] = modifiedEP

    this.setState(
      {
        selectedHandler: modifiedEP,
        handlersOnThisAlert: [...handlersOnThisAlert],
      },
      this.handleUpdateAllAlerts
    )
  }

  render() {
    const {handlersFromConfig, configLink, rule, ruleActions} = this.props
    const {handlersOnThisAlert, selectedHandler} = this.state
    const alerts = _.map([...DEFAULT_HANDLERS, ...handlersFromConfig], a => {
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
              className="dropdown-170 rule-message--add-endpoint"
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
                  updateDetails={ruleActions.updateDetails}
                  rule={rule}
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
  ruleActions: shape({
    updateAlertNodes: func.isRequired,
    updateMessage: func.isRequired,
    updateDetails: func.isRequired,
    updateAlertProperty: func.isRequired,
  }).isRequired,
  handlersFromConfig: arrayOf(shape({})),
  configLink: string,
}

export default RuleHandlers
