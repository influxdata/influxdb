import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import HandlerOptions from 'src/kapacitor/components/HandlerOptions'
import HandlerTabs from 'src/kapacitor/components/HandlerTabs'
import Dropdown from 'shared/components/Dropdown'
import {parseHandlersFromRule} from 'src/shared/parsing/parseHandlersFromRule'

import {DEFAULT_HANDLERS} from 'src/kapacitor/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RuleHandlers extends Component {
  constructor(props) {
    super(props)
    const {handlersFromConfig} = this.props
    const {
      handlersOnThisAlert,
      selectedHandler,
      handlersOfKind,
    } = parseHandlersFromRule(this.props.rule, handlersFromConfig)

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

  handleAddHandler = selectedItem => {
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

  handleRemoveHandler = removedHandler => e => {
    e.stopPropagation()
    const {handlersOnThisAlert, selectedHandler} = this.state
    const removedIndex = _.findIndex(handlersOnThisAlert, [
      'alias',
      removedHandler.alias,
    ])
    const remainingHandlers = _.reject(handlersOnThisAlert, [
      'alias',
      removedHandler.alias,
    ])
    if (selectedHandler.alias === removedHandler.alias) {
      const selectedIndex = removedIndex > 0 ? removedIndex - 1 : 0
      const newSelected = remainingHandlers.length
        ? remainingHandlers[selectedIndex]
        : null
      this.setState({selectedHandler: newSelected})
    }
    this.setState(
      {handlersOnThisAlert: remainingHandlers},
      this.handleUpdateAllAlerts
    )
  }

  handleUpdateAllAlerts = () => {
    const {rule, ruleActions} = this.props
    const {handlersOnThisAlert} = this.state

    ruleActions.updateAlertNodes(rule.id, handlersOnThisAlert)
  }

  handleModifyHandler = (selectedHandler, fieldName, parseToArray) => e => {
    const {handlersOnThisAlert} = this.state
    let modifiedHandler

    if (e.target.type === 'checkbox') {
      modifiedHandler = {
        ...selectedHandler,
        [fieldName]: !selectedHandler[fieldName],
      }
    } else if (parseToArray) {
      modifiedHandler = {
        ...selectedHandler,
        [fieldName]: _.split(e.target.value, ' '),
      }
    } else {
      modifiedHandler = {
        ...selectedHandler,
        [fieldName]: e.target.value,
      }
    }

    const modifiedIndex = _.findIndex(handlersOnThisAlert, [
      'alias',
      modifiedHandler.alias,
    ])

    handlersOnThisAlert[modifiedIndex] = modifiedHandler

    this.setState(
      {
        selectedHandler: modifiedHandler,
        handlersOnThisAlert: [...handlersOnThisAlert],
      },
      this.handleUpdateAllAlerts
    )
  }

  getNickname = handler => {
    const configType = handler.type
    if (configType === 'slack') {
      const workspace = _.get(handler, 'workspace')

      if (workspace === '') {
        return 'default'
      }

      return workspace
    }

    return undefined
  }

  mapWithNicknames = handlers => {
    return _.map(handlers, h => {
      const nickname = this.getNickname(h)
      if (nickname) {
        return {...h, text: `${h.type} (${nickname})`}
      }

      return {...h, text: h.type}
    })
  }

  render() {
    const {
      rule,
      ruleActions,
      onGoToConfig,
      validationError,
      handlersFromConfig,
    } = this.props
    const {handlersOnThisAlert, selectedHandler} = this.state

    const mappedHandlers = this.mapWithNicknames([
      ...DEFAULT_HANDLERS,
      ...handlersFromConfig,
    ])

    const mappedHandlersOnThisAlert = this.mapWithNicknames(handlersOnThisAlert)

    const handlers = _.flatten([
      _.filter(mappedHandlers, ['enabled', true]),
      {text: 'SEPARATOR'},
      _.filter(mappedHandlers, ['enabled', false]),
    ])

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
              items={handlers}
              menuClass="dropdown-malachite"
              selected={dropdownLabel}
              onChoose={this.handleAddHandler}
              className="dropdown-170 rule-message--add-endpoint"
            />
          </div>
          {mappedHandlersOnThisAlert.length ? (
            <div className="rule-message--endpoints">
              <HandlerTabs
                handlersOnThisAlert={mappedHandlersOnThisAlert}
                selectedHandler={selectedHandler}
                handleChooseHandler={this.handleChooseHandler}
                handleRemoveHandler={this.handleRemoveHandler}
              />
              <HandlerOptions
                selectedHandler={selectedHandler}
                handleModifyHandler={this.handleModifyHandler}
                updateDetails={ruleActions.updateDetails}
                rule={rule}
                onGoToConfig={onGoToConfig}
                validationError={validationError}
              />
            </div>
          ) : null}
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
  }).isRequired,
  handlersFromConfig: arrayOf(shape({})),
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default RuleHandlers
