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
    const {selectedEndpoint, handleModifyEndpoint} = this.props
    switch (selectedEndpoint && selectedEndpoint.type) {
      case 'post':
        return (
          <PostConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'tcp':
        return (
          <TcpConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'exec':
        return (
          <ExecConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'log':
        return (
          <LogConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'smtp':
        return (
          <SmtpConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'alerta':
        return (
          <AlertaConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'hipchat':
        return (
          <HipchatConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'opsgenie':
        return (
          <OpsgenieConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'pagerduty':
        return (
          <PagerdutyConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'pushover':
        return (
          <PushoverConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'sensu':
        return (
          <SensuConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'slack':
        return (
          <SlackConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'talk':
        return (
          <TalkConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'telegram':
        return (
          <TelegramConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
          />
        )
      case 'victorops':
        return (
          <VictoropsConfig
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
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
