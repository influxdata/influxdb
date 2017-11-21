import React, {Component, PropTypes} from 'react'
import {
  PostHandler,
  TcpHandler,
  ExecHandler,
  LogHandler,
  SmtpHandler,
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
    const {selectedHandler, handleModifyHandler, configLink} = this.props
    switch (selectedHandler && selectedHandler.type) {
      case 'post':
        return (
          <PostHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'tcp':
        return (
          <TcpHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'exec':
        return (
          <ExecHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'log':
        return (
          <LogHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'smtp':
        return (
          <SmtpHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'alerta':
        return (
          <AlertaHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'hipchat':
        return (
          <HipchatHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'opsgenie':
        return (
          <OpsgenieHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'pagerduty':
        return (
          <PagerdutyHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'pushover':
        return (
          <PushoverHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'sensu':
        return (
          <SensuHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'slack':
        return (
          <SlackHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'talk':
        return (
          <TalkHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'telegram':
        return (
          <TelegramHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
          />
        )
      case 'victorops':
        return (
          <VictoropsHandler
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            configLink={configLink}
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
  configLink: string,
}

export default HandlerOptions
