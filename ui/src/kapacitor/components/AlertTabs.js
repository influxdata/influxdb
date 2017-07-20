import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'shared/components/Tabs'
import {
  getKapacitorConfig,
  updateKapacitorConfigSection,
  testAlertOutput,
} from 'shared/apis'

import {
  AlertaConfig,
  HipChatConfig,
  OpsGenieConfig,
  PagerDutyConfig,
  SensuConfig,
  SlackConfig,
  SMTPConfig,
  TalkConfig,
  TelegramConfig,
  VictorOpsConfig,
} from './config'

class AlertTabs extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedEndpoint: 'smtp',
      configSections: null,
    }
    this.refreshKapacitorConfig = ::this.refreshKapacitorConfig
    this.getSection = ::this.getSection
    this.handleSaveConfig = ::this.handleSaveConfig
    this.handleTest = ::this.handleTest
    this.sanitizeProperties = ::this.sanitizeProperties
  }

  componentDidMount() {
    this.refreshKapacitorConfig(this.props.kapacitor)
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.kapacitor.url !== nextProps.kapacitor.url) {
      this.refreshKapacitorConfig(nextProps.kapacitor)
    }
  }

  refreshKapacitorConfig(kapacitor) {
    getKapacitorConfig(kapacitor)
      .then(({data: {sections}}) => {
        this.setState({configSections: sections})
      })
      .catch(() => {
        this.setState({configSections: null})
        this.props.addFlashMessage({
          type: 'error',
          text: 'There was an error getting the Kapacitor config',
        })
      })
  }

  getSection(sections, section) {
    return _.get(sections, [section, 'elements', '0'], null)
  }

  handleSaveConfig(section, properties) {
    if (section !== '') {
      const propsToSend = this.sanitizeProperties(section, properties)
      updateKapacitorConfigSection(this.props.kapacitor, section, propsToSend)
        .then(() => {
          this.refreshKapacitorConfig(this.props.kapacitor)
          this.props.addFlashMessage({
            type: 'success',
            text: `Alert for ${section} successfully saved`,
          })
        })
        .catch(() => {
          this.props.addFlashMessage({
            type: 'error',
            text: 'There was an error saving the kapacitor config',
          })
        })
    }
  }

  handleTest(section, properties) {
    const propsToSend = this.sanitizeProperties(section, properties)
    testAlertOutput(this.props.kapacitor, section, propsToSend)
      .then(() => {
        this.props.addFlashMessage({
          type: 'success',
          text: 'Slack test message sent',
        })
      })
      .catch(() => {
        this.props.addFlashMessage({
          type: 'error',
          text: 'There was an error testing the slack alert',
        })
      })
  }

  sanitizeProperties(section, properties) {
    const cleanProps = Object.assign({}, properties, {enabled: true})
    const {redacted} = this.getSection(this.state.configSections, section)
    if (redacted && redacted.length) {
      redacted.forEach(badProp => {
        if (properties[badProp] === 'true') {
          delete cleanProps[badProp]
        }
      })
    }
    return cleanProps
  }

  render() {
    const {configSections} = this.state

    if (!configSections) {
      return null
    }

    const test = properties => {
      this.handleTest('slack', properties)
    }

    const tabs = [
      {
        type: 'Alerta',
        component: (
          <AlertaConfig
            onSave={p => this.handleSaveConfig('alerta', p)}
            config={this.getSection(configSections, 'alerta')}
          />
        ),
      },
      {
        type: 'SMTP',
        component: (
          <SMTPConfig
            onSave={p => this.handleSaveConfig('smtp', p)}
            config={this.getSection(configSections, 'smtp')}
          />
        ),
      },
      {
        type: 'Slack',
        component: (
          <SlackConfig
            onSave={p => this.handleSaveConfig('slack', p)}
            onTest={test}
            config={this.getSection(configSections, 'slack')}
          />
        ),
      },
      {
        type: 'VictorOps',
        component: (
          <VictorOpsConfig
            onSave={p => this.handleSaveConfig('victorops', p)}
            config={this.getSection(configSections, 'victorops')}
          />
        ),
      },
      {
        type: 'Telegram',
        component: (
          <TelegramConfig
            onSave={p => this.handleSaveConfig('telegram', p)}
            config={this.getSection(configSections, 'telegram')}
          />
        ),
      },
      {
        type: 'OpsGenie',
        component: (
          <OpsGenieConfig
            onSave={p => this.handleSaveConfig('opsgenie', p)}
            config={this.getSection(configSections, 'opsgenie')}
          />
        ),
      },
      {
        type: 'PagerDuty',
        component: (
          <PagerDutyConfig
            onSave={p => this.handleSaveConfig('pagerduty', p)}
            config={this.getSection(configSections, 'pagerduty')}
          />
        ),
      },
      {
        type: 'HipChat',
        component: (
          <HipChatConfig
            onSave={p => this.handleSaveConfig('hipchat', p)}
            config={this.getSection(configSections, 'hipchat')}
          />
        ),
      },
      {
        type: 'Sensu',
        component: (
          <SensuConfig
            onSave={p => this.handleSaveConfig('sensu', p)}
            config={this.getSection(configSections, 'sensu')}
          />
        ),
      },
      {
        type: 'Talk',
        component: (
          <TalkConfig
            onSave={p => this.handleSaveConfig('talk', p)}
            config={this.getSection(configSections, 'talk')}
          />
        ),
      },
    ]

    return (
      <div>
        <div className="panel panel-minimal">
          <div className="panel-heading u-flex u-ai-center u-jc-space-between">
            <h2 className="panel-title">Configure Alert Endpoints</h2>
          </div>
        </div>

        <Tabs tabContentsClass="config-endpoint">
          <TabList customClass="config-endpoint--tabs">
            {tabs.map((t, i) =>
              <Tab key={tabs[i].type}>
                {tabs[i].type}
              </Tab>
            )}
          </TabList>
          <TabPanels customClass="config-endpoint--tab-contents">
            {tabs.map((t, i) =>
              <TabPanel key={tabs[i].type}>
                {t.component}
              </TabPanel>
            )}
          </TabPanels>
        </Tabs>
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

AlertTabs.propTypes = {
  source: shape({
    id: string.isRequired,
  }).isRequired,
  kapacitor: shape({
    url: string.isRequired,
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }),
  addFlashMessage: func.isRequired,
}

export default AlertTabs
