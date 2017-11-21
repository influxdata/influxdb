import React, {Component, PropTypes} from 'react'
import {
  PostConfig,
  TcpConfig,
  ExecConfig,
  LogConfig,
  SmtpConfig,
  AlertaConfig,
  HipchatConfig,
  OpsgenieConfig,
  PagerdutyConfig,
  PushoverConfig,
  SensuConfig,
  SlackConfig,
  TalkConfig,
  TelegramConfig,
  VictoropsConfig,
} from './configEP'

class EndpointOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {selectedEndpoint, handleModifyEndpoint, configLink} = this.props
    switch (selectedEndpoint && selectedEndpoint.type) {
      case 'post':
        return (
          <PostConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'tcp':
        return (
          <TcpConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'exec':
        return (
          <ExecConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'log':
        return (
          <LogConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'smtp':
        return (
          <SmtpConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'alerta':
        return (
          <AlertaConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'hipchat':
        return (
          <HipchatConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'opsgenie':
        return (
          <OpsgenieConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'pagerduty':
        return (
          <PagerdutyConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'pushover':
        return (
          <PushoverConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'sensu':
        return (
          <SensuConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'slack':
        return (
          <SlackConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'talk':
        return (
          <TalkConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'telegram':
        return (
          <TelegramConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      case 'victorops':
        return (
          <VictoropsConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            configLink={configLink}
          />
        )
      default:
        return null
    }
  }
}

const {func, shape} = PropTypes

EndpointOptions.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default EndpointOptions
