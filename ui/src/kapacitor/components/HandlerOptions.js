import React, {Component, PropTypes} from 'react'
import {
  PostHandler,
  TcpHandler,
  ExecHandler,
  LogHandler,
  EmailHandler,
  AlertaHandler,
  HipchatHandler,
  OpsgenieHandler,
  PagerdutyHandler,
  PushoverHandler,
  SensuHandler,
  SlackHandler,
  TalkHandler,
  TelegramHandler,
  VictoropsHandler,
} from './handlers'

class HandlerOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {
      selectedHandler,
      handleModifyHandler,
      rule,
      updateDetails,
      onGoToConfig,
      validationError,
    } = this.props
    switch (selectedHandler && selectedHandler.type) {
      case 'post':
        return (
          <PostHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
          />
        )
      case 'tcp':
        return (
          <TcpHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
          />
        )
      case 'exec':
        return (
          <ExecHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
          />
        )
      case 'log':
        return (
          <LogHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
          />
        )
      case 'email':
        return (
          <EmailHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('smtp')}
            validationError={validationError}
            updateDetails={updateDetails}
            rule={rule}
          />
        )
      case 'alerta':
        return (
          <AlertaHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('alerta')}
            validationError={validationError}
          />
        )
      case 'hipChat':
        return (
          <HipchatHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('hipchat')}
            validationError={validationError}
          />
        )
      case 'opsGenie':
        return (
          <OpsgenieHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('opsgenie')}
            validationError={validationError}
          />
        )
      case 'pagerDuty':
        return (
          <PagerdutyHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('pagerduty')}
            validationError={validationError}
          />
        )
      case 'pushover':
        return (
          <PushoverHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('pushover')}
            validationError={validationError}
          />
        )
      case 'sensu':
        return (
          <SensuHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('sensu')}
            validationError={validationError}
          />
        )
      case 'slack':
        return (
          <SlackHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('slack')}
            validationError={validationError}
          />
        )
      case 'talk':
        return (
          <TalkHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('talk')}
            validationError={validationError}
          />
        )
      case 'telegram':
        return (
          <TelegramHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('telegram')}
            validationError={validationError}
          />
        )
      case 'victorOps':
        return (
          <VictoropsHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('victorops')}
            validationError={validationError}
          />
        )
      default:
        return null
    }
  }
}

const {func, shape, string} = PropTypes

HandlerOptions.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  updateDetails: func,
  rule: shape({}),
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default HandlerOptions
