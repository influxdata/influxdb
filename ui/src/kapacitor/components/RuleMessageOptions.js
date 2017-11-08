import React, {Component, PropTypes} from 'react'
import {
  HttpConfig,
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

class RuleMessageOptions extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {selectedEndpoint} = this.props
    switch (selectedEndpoint && selectedEndpoint.type) {
      case 'http':
        return <HttpConfig />
      case 'tcp':
        return <TcpConfig />
      case 'exec':
        return <ExecConfig />
      case 'log':
        return <LogConfig />
      case 'smtp':
        return <SmtpConfig />
      case 'alerta':
        return <AlertaConfig />
      case 'hipchat':
        return <HipchatConfig />
      case 'opsgenie':
        return <OpsgenieConfig />
      case 'pagerduty':
        return <PagerdutyConfig />
      case 'pushover':
        return <PushoverConfig />
      case 'sensu':
        return <SensuConfig />
      case 'slack':
        return <SlackConfig />
      case 'talk':
        return <TalkConfig />
      case 'telegram':
        return <TelegramConfig />
      case 'victorops':
        return <VictoropsConfig />
      default:
        return null
    }
  }
}

const {func, shape} = PropTypes

RuleMessageOptions.propTypes = {}

export default RuleMessageOptions
