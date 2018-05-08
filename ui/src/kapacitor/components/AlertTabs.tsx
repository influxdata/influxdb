import React, {PureComponent, MouseEvent} from 'react'

import _ from 'lodash'

import {
  Tab,
  Tabs,
  TabPanel,
  TabPanels,
  TabList,
} from 'src/shared/components/Tabs'
import {
  getKapacitorConfig,
  updateKapacitorConfigSection,
  addKapacitorConfigInSection,
  testAlertOutput,
  getAllServices,
} from 'src/shared/apis'

import {
  AlertaConfig,
  HipChatConfig,
  KafkaConfig,
  OpsGenieConfig,
  PagerDutyConfig,
  PagerDuty2Config,
  PushoverConfig,
  SensuConfig,
  SMTPConfig,
  TalkConfig,
  TelegramConfig,
  VictorOpsConfig,
} from './config'

import {
  notifyRefreshKapacitorFailed,
  notifyAlertEndpointSaved,
  notifyAlertEndpointSaveFailed,
  notifyTestAlertSent,
  notifyTestAlertFailed,
  notifyCouldNotRetrieveKapacitorServices,
} from 'src/shared/copy/notifications'
import DeprecationWarning from 'src/admin/components/DeprecationWarning'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source, Kapacitor} from 'src/types'
import SlackConfigs from 'src/kapacitor/components/config/SlackConfigs'

interface Service {
  link: Link
  name: string
  options: {
    id: string
  }
}

interface Link {
  rel: string
  href: string
}

interface Element {
  link: Link
  options: any
  redacted: string[]
}

interface Section {
  link: string
  elements: Element[]
}

interface Sections {
  alerta: Section
  hipchat: Section
  httppost: Section
  influxdb: Section
  kafka: Section
  mqtt: Section
  opsgenie: Section
  opsgenie2: Section
  pagerduty: Section
  pagerduty2: Section
  pushover: Section
  sensu: Section
  slack: Section
  smtp: Section
  snmptrap: Section
  talk: Section
  telegram: Section
  victorops: Section
}

interface Config {
  type: string
  enabled: boolean
  renderComponent: () => JSX.Element
}

interface SupportedConfig {
  alerta: Config
  hipchat: Config
  kafka: Config
  opsgenie: Config
  opsgenie2: Config
  pagerduty: Config
  pagerduty2: Config
  pushover: Config
  sensu: Config
  slack: Config
  smtp: Config
  talk: Config
  telegram: Config
  victorops: Config
}

interface Notification {
  id?: string
  type: string
  icon: string
  duration: number
  message: string
}

interface Props {
  source: Source
  kapacitor: Kapacitor
  notify: (message: Notification) => void
  hash: string
}

interface State {
  configSections: Sections
  services: Service[]
}

@ErrorHandling
class AlertTabs extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      configSections: null,
      services: [],
    }
  }

  public async componentDidMount() {
    const {kapacitor} = this.props
    try {
      this.refreshKapacitorConfig(kapacitor)
      const services: Service[] = await getAllServices(kapacitor)
      this.setState({services})
    } catch (error) {
      this.setState({services: null})
      this.props.notify(notifyCouldNotRetrieveKapacitorServices(kapacitor))
    }
  }

  public componentWillReceiveProps(nextProps) {
    if (this.props.kapacitor.url !== nextProps.kapacitor.url) {
      this.refreshKapacitorConfig(nextProps.kapacitor)
    }
  }

  public render() {
    const {configSections} = this.state
    const {hash} = this.props

    if (!configSections) {
      return null
    }

    const pagerDutyV1Enabled: boolean = this.getEnabled(
      configSections,
      'pagerduty'
    )
    const opsGenieV1Enabled: boolean = this.getEnabled(
      configSections,
      'opsgenie'
    )

    const pagerDutyDeprecationMessage: JSX.Element = (
      <div>
        PagerDuty v1 is being{' '}
        {
          <a
            href="https://v2.developer.pagerduty.com/docs/v1-rest-api-decommissioning-faq"
            target="_blank"
          >
            deprecated
          </a>
        }
        . Please update your Kapacitor and configure PagerDuty v2.
      </div>
    )

    const opsGenieDeprecationMessage: JSX.Element = (
      <div>
        OpsGenie v1 is being deprecated. Please update your Kapacitor and
        configure OpsGenie v2.
      </div>
    )

    const supportedConfigs: SupportedConfig = {
      alerta: {
        type: 'Alerta',
        enabled: this.getEnabled(configSections, 'alerta'),
        renderComponent: () => (
          <AlertaConfig
            onSave={this.handleSaveConfig('alerta')}
            config={this.getSectionElement(configSections, 'alerta')}
            onTest={this.handleTestConfig('alerta')}
            enabled={this.getEnabled(configSections, 'alerta')}
          />
        ),
      },
      hipchat: {
        type: 'HipChat',
        enabled: this.getEnabled(configSections, 'hipchat'),
        renderComponent: () => (
          <HipChatConfig
            onSave={this.handleSaveConfig('hipchat')}
            config={this.getSectionElement(configSections, 'hipchat')}
            onTest={this.handleTestConfig('hipchat')}
            enabled={this.getEnabled(configSections, 'hipchat')}
          />
        ),
      },
      kafka: {
        type: 'Kafka',
        enabled: this.getEnabled(configSections, 'kafka'),
        renderComponent: () => (
          <KafkaConfig
            onSave={this.handleSaveConfig('kafka')}
            config={this.getSection(configSections, 'kafka')}
            onTest={this.handleTestConfig('kafka', {
              cluster: this.getProperty(configSections, 'kafka', 'id'),
            })}
            enabled={this.getEnabled(configSections, 'kafka')}
            notify={this.props.notify}
          />
        ),
      },
      opsgenie: {
        type: 'OpsGenie',
        enabled: this.getEnabled(configSections, 'opsgenie'),
        renderComponent: () => (
          <OpsGenieConfig
            onSave={this.handleSaveConfig('opsgenie')}
            config={this.getSectionElement(configSections, 'opsgenie')}
            onTest={this.handleTestConfig('opsgenie')}
            enabled={this.getEnabled(configSections, 'opsgenie')}
          />
        ),
      },
      opsgenie2: {
        type: 'OpsGenie2',
        enabled: this.getEnabled(configSections, 'opsgenie2'),
        renderComponent: () => (
          <OpsGenieConfig
            onSave={this.handleSaveConfig('opsgenie2')}
            config={this.getSectionElement(configSections, 'opsgenie2')}
            onTest={this.handleTestConfig('opsgenie2')}
            enabled={this.getEnabled(configSections, 'opsgenie2')}
          />
        ),
      },
      pagerduty: {
        type: 'PagerDuty',
        enabled: this.getEnabled(configSections, 'pagerduty'),
        renderComponent: () => (
          <PagerDutyConfig
            onSave={this.handleSaveConfig('pagerduty')}
            config={this.getSectionElement(configSections, 'pagerduty')}
            onTest={this.handleTestConfig('pagerduty')}
            enabled={this.getEnabled(configSections, 'pagerduty')}
          />
        ),
      },
      pagerduty2: {
        type: 'PagerDuty2',
        enabled: this.getEnabled(configSections, 'pagerduty2'),
        renderComponent: () => (
          <PagerDuty2Config
            onSave={this.handleSaveConfig('pagerduty2')}
            config={this.getSectionElement(configSections, 'pagerduty2')}
            onTest={this.handleTestConfig('pagerduty2')}
            enabled={this.getEnabled(configSections, 'pagerduty2')}
          />
        ),
      },
      pushover: {
        type: 'Pushover',
        enabled: this.getEnabled(configSections, 'pushover'),
        renderComponent: () => (
          <PushoverConfig
            onSave={this.handleSaveConfig('pushover')}
            config={this.getSectionElement(configSections, 'pushover')}
            onTest={this.handleTestConfig('pushover')}
            enabled={this.getEnabled(configSections, 'pushover')}
          />
        ),
      },
      sensu: {
        type: 'Sensu',
        enabled: this.getEnabled(configSections, 'sensu'),
        renderComponent: () => (
          <SensuConfig
            onSave={this.handleSaveConfig('sensu')}
            config={this.getSectionElement(configSections, 'sensu')}
            onTest={this.handleTestConfig('sensu')}
            enabled={this.getEnabled(configSections, 'sensu')}
          />
        ),
      },
      slack: {
        type: 'Slack',
        enabled: this.getEnabled(configSections, 'slack'),
        renderComponent: () => (
          <SlackConfigs
            slackConfigs={this.getSectionElements(configSections, 'slack')}
            onSave={this.handleSaveConfig('slack')}
            config={this.getSectionElement(configSections, 'slack')}
            onTest={this.handleTestConfig('slack')}
            enabled={this.getEnabled(configSections, 'slack')}
          />
        ),
      },
      // slack: {
      //   type: 'Slack',
      //   enabled: this.getEnabled(configSections, 'slack'),
      //   renderComponent: () => (
      //     <SlackConfig
      //       onSave={this.handleSaveConfig('slack')}
      //       config={this.getSectionElement(configSections, 'slack')}
      //       onTest={this.handleTestConfig('slack')}
      //       enabled={this.getEnabled(configSections, 'slack')}
      //     />
      //   ),
      // },
      smtp: {
        type: 'SMTP',
        enabled: this.getEnabled(configSections, 'smtp'),
        renderComponent: () => (
          <SMTPConfig
            onSave={this.handleSaveConfig('smtp')}
            config={this.getSectionElement(configSections, 'smtp')}
            onTest={this.handleTestConfig('smtp')}
            enabled={this.getEnabled(configSections, 'smtp')}
          />
        ),
      },
      talk: {
        type: 'Talk',
        enabled: this.getEnabled(configSections, 'talk'),
        renderComponent: () => (
          <TalkConfig
            onSave={this.handleSaveConfig('talk')}
            config={this.getSectionElement(configSections, 'talk')}
            onTest={this.handleTestConfig('talk')}
            enabled={this.getEnabled(configSections, 'talk')}
          />
        ),
      },
      telegram: {
        type: 'Telegram',
        enabled: this.getEnabled(configSections, 'telegram'),
        renderComponent: () => (
          <TelegramConfig
            onSave={this.handleSaveConfig('telegram')}
            config={this.getSectionElement(configSections, 'telegram')}
            onTest={this.handleTestConfig('telegram')}
            enabled={this.getEnabled(configSections, 'telegram')}
          />
        ),
      },
      victorops: {
        type: 'VictorOps',
        enabled: this.getEnabled(configSections, 'victorops'),
        renderComponent: () => (
          <VictorOpsConfig
            onSave={this.handleSaveConfig('victorops')}
            config={this.getSectionElement(configSections, 'victorops')}
            onTest={this.handleTestConfig('victorops')}
            enabled={this.getEnabled(configSections, 'victorops')}
          />
        ),
      },
    }
    return (
      <div className="panel">
        <div className="panel-heading">
          <h2 className="panel-title">Configure Alert Endpoints</h2>
        </div>
        {pagerDutyV1Enabled && (
          <DeprecationWarning message={pagerDutyDeprecationMessage} />
        )}
        {opsGenieV1Enabled && (
          <DeprecationWarning message={opsGenieDeprecationMessage} />
        )}

        <Tabs
          tabContentsClass="config-endpoint"
          initialIndex={this.getInitialIndex(supportedConfigs, hash)}
        >
          <TabList customClass="config-endpoint--tabs">
            {_.reduce(
              configSections,
              (acc, __, k) => {
                return this.isSupportedService(supportedConfigs[k])
                  ? acc.concat(
                      <Tab
                        key={supportedConfigs[k].type}
                        isConfigured={supportedConfigs[k].enabled}
                      >
                        {supportedConfigs[k].type}
                      </Tab>
                    )
                  : acc
              },
              []
            )}
          </TabList>
          <TabPanels customClass="config-endpoint--tab-contents">
            {_.reduce(
              configSections,
              (acc, __, k) =>
                this.isSupportedService(supportedConfigs[k])
                  ? acc.concat(
                      <TabPanel key={supportedConfigs[k].type}>
                        {supportedConfigs[k].renderComponent()}
                      </TabPanel>
                    )
                  : acc,
              []
            )}
          </TabPanels>
        </Tabs>
      </div>
    )
  }

  private refreshKapacitorConfig = async (
    kapacitor: Kapacitor
  ): Promise<void> => {
    try {
      const {
        data: {sections},
      } = await getKapacitorConfig(kapacitor)
      this.setState({configSections: sections})
    } catch (error) {
      this.setState({configSections: null})
      this.props.notify(notifyRefreshKapacitorFailed())
    }
  }

  private getSectionElement = (
    sections: Sections,
    section: string,
    elementIndex: number = 0
  ): Element => {
    return _.get(sections, [section, 'elements', elementIndex], null)
  }

  private getSectionElements = (
    sections: Sections,
    section: string
  ): Element[] => {
    return _.get(sections, [section, 'elements'], null)
  }

  private getEnabled = (sections: Sections, section: string): boolean => {
    return _.get(
      sections,
      [section, 'elements', '0', 'options', 'enabled'],
      false
    )
  }

  private getProperty = (
    sections: Sections,
    section: string,
    property: string
  ): boolean => {
    return _.get(
      sections,
      [section, 'elements', '0', 'options', property],
      null
    )
  }

  private handleSaveConfig = (section: string) => async (
    properties
  ): Promise<boolean> => {
    if (section !== '') {
      const propsToSend = this.sanitizeProperties(section, properties)
      try {
        await updateKapacitorConfigSection(
          this.props.kapacitor,
          section,
          propsToSend
        )
        this.refreshKapacitorConfig(this.props.kapacitor)
        this.props.notify(notifyAlertEndpointSaved(section))
        return true
      } catch ({
        data: {error},
      }) {
        const errorMsg = error.split(': ').pop()
        this.props.notify(notifyAlertEndpointSaveFailed(section, errorMsg))
        return false
      }
    }
  }
  private handleTestConfig = (section: string, options?: object) => async (
    e: MouseEvent<HTMLButtonElement>
  ): Promise<void> => {
    e.preventDefault()

    try {
      const {data} = await testAlertOutput(
        this.props.kapacitor,
        section,
        options
      )
      if (data.success) {
        this.props.notify(notifyTestAlertSent(section))
      } else {
        this.props.notify(notifyTestAlertFailed(section, data.message))
      }
    } catch (error) {
      this.props.notify(notifyTestAlertFailed(section))
    }
  }

  private sanitizeProperties = (section: string, properties: Props): Props => {
    const cleanProps = {enabled: true, ...properties}
    const {redacted} = this.getSectionElement(
      this.state.configSections,
      section
    )
    if (redacted && redacted.length) {
      redacted.forEach(badProp => {
        if (properties[badProp] === 'true') {
          delete cleanProps[badProp]
        }
      })
    }

    return cleanProps
  }

  private getInitialIndex = (
    supportedConfigs: SupportedConfig,
    hash: string
  ): number => {
    const index = _.indexOf(_.keys(supportedConfigs), _.replace(hash, '#', ''))
    return index >= 0 ? index : 0
  }

  private isSupportedService = config => {
    return (
      config &&
      this.state.services.find(service => {
        return service.name === _.toLower(config.type)
      })
    )
  }
}

export default AlertTabs
