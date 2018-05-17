import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {
  PostHandler,
  TcpHandler,
  ExecHandler,
  LogHandler,
  EmailHandler,
  AlertaHandler,
  HipchatHandler,
  KafkaHandler,
  OpsgenieHandler,
  PagerdutyHandler,
  Pagerduty2Handler,
  PushoverHandler,
  SensuHandler,
  SlackHandler,
  TalkHandler,
  TelegramHandler,
  VictoropsHandler,
} from 'src/kapacitor/components/handlers'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
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
      case 'kafka':
        return (
          <KafkaHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('kafka')}
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
      case 'opsGenie2':
        return (
          <OpsgenieHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('opsgenie2')}
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
      case 'pagerDuty2':
        return (
          <Pagerduty2Handler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            onGoToConfig={onGoToConfig('pagerduty2')}
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
