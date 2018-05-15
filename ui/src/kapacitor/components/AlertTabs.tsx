import React, {PureComponent, MouseEvent} from 'react'

import _ from 'lodash'
import {get} from 'src/utils/wrappers'

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
  deleteKapacitorConfigInSection,
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
  notifyAlertEndpointDeleteFailed,
  notifyAlertEndpointDeleted,
  notifyTestAlertSent,
  notifyTestAlertFailed,
  notifyCouldNotRetrieveKapacitorServices,
} from 'src/shared/copy/notifications'
import DeprecationWarning from 'src/admin/components/DeprecationWarning'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {Source, Kapacitor} from 'src/types'
import {ServiceProperties, SpecificConfigOptions} from 'src/types/kapacitor'
import SlackConfigs from 'src/kapacitor/components/config/SlackConfigs'
import {
  AlertDisplayText,
  SupportedServices,
  AlertTypes,
} from 'src/kapacitor/constants'

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

    const pagerDutyV1Enabled: boolean = this.getConfigEnabled(
      configSections,
      AlertTypes.pagerduty
    )
    const opsGenieV1Enabled: boolean = this.getConfigEnabled(
      configSections,
      AlertTypes.opsgenie
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
          initialIndex={this.getInitialIndex(hash)}
        >
          <TabList customClass="config-endpoint--tabs">
            {_.reduce(
              configSections,
              (acc, __, k) => {
                if (this.isSupportedService(k)) {
                  return acc.concat(
                    <Tab
                      key={k}
                      isConfigured={this.getConfigEnabled(configSections, k)}
                    >
                      {AlertDisplayText[k]}
                    </Tab>
                  )
                }
                return acc
              },
              []
            )}
          </TabList>
          <TabPanels customClass="config-endpoint--tab-contents">
            {_.reduce(
              configSections,
              (acc, __, k) => {
                if (this.isSupportedService(k)) {
                  return acc.concat(
                    <TabPanel key={k}>{this.getConfig(k)}</TabPanel>
                  )
                }
                return acc
              },
              []
            )}
          </TabPanels>
        </Tabs>
      </div>
    )
  }

  private getConfig(config: string): JSX.Element {
    const {configSections} = this.state
    switch (config) {
      case AlertTypes.alerta:
        return (
          <AlertaConfig
            onSave={this.handleSaveConfig(AlertTypes.alerta)}
            config={this.getSectionElement(configSections, AlertTypes.alerta)}
            onTest={this.handleTestConfig(AlertTypes.alerta)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.alerta)}
          />
        )
      case AlertTypes.hipchat:
        return (
          <HipChatConfig
            onSave={this.handleSaveConfig(AlertTypes.hipchat)}
            config={this.getSectionElement(configSections, AlertTypes.hipchat)}
            onTest={this.handleTestConfig(AlertTypes.hipchat)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.hipchat)}
          />
        )
      case AlertTypes.kafka:
        return (
          <KafkaConfig
            onSave={this.handleSaveConfig(AlertTypes.kafka)}
            config={this.getSectionElement(configSections, AlertTypes.kafka)}
            onTest={this.handleTestConfig(AlertTypes.kafka, {
              cluster: this.getProperty(configSections, AlertTypes.kafka, 'id'),
            })}
            enabled={this.getConfigEnabled(configSections, AlertTypes.kafka)}
            notify={this.props.notify}
          />
        )
      case AlertTypes.opsgenie:
        return (
          <OpsGenieConfig
            onSave={this.handleSaveConfig(AlertTypes.opsgenie)}
            config={this.getSectionElement(configSections, AlertTypes.opsgenie)}
            onTest={this.handleTestConfig(AlertTypes.opsgenie)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.opsgenie)}
          />
        )
      case AlertTypes.opsgenie2:
        return (
          <OpsGenieConfig
            onSave={this.handleSaveConfig(AlertTypes.opsgenie2)}
            config={this.getSectionElement(
              configSections,
              AlertTypes.opsgenie2
            )}
            onTest={this.handleTestConfig(AlertTypes.opsgenie2)}
            enabled={this.getConfigEnabled(
              configSections,
              AlertTypes.opsgenie2
            )}
          />
        )
      case AlertTypes.pagerduty:
        return (
          <PagerDutyConfig
            onSave={this.handleSaveConfig(AlertTypes.pagerduty)}
            config={this.getSectionElement(
              configSections,
              AlertTypes.pagerduty
            )}
            onTest={this.handleTestConfig(AlertTypes.pagerduty)}
            enabled={this.getConfigEnabled(
              configSections,
              AlertTypes.pagerduty
            )}
          />
        )
      case AlertTypes.pagerduty2:
        return (
          <PagerDuty2Config
            onSave={this.handleSaveConfig(AlertTypes.pagerduty2)}
            config={this.getSectionElement(
              configSections,
              AlertTypes.pagerduty2
            )}
            onTest={this.handleTestConfig(AlertTypes.pagerduty2)}
            enabled={this.getConfigEnabled(
              configSections,
              AlertTypes.pagerduty2
            )}
          />
        )
      case AlertTypes.pushover:
        return (
          <PushoverConfig
            onSave={this.handleSaveConfig(AlertTypes.pushover)}
            config={this.getSectionElement(configSections, AlertTypes.pushover)}
            onTest={this.handleTestConfig(AlertTypes.pushover)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.pushover)}
          />
        )
      case AlertTypes.sensu:
        return (
          <SensuConfig
            onSave={this.handleSaveConfig(AlertTypes.sensu)}
            config={this.getSectionElement(configSections, AlertTypes.sensu)}
            onTest={this.handleTestConfig(AlertTypes.sensu)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.sensu)}
          />
        )
      case AlertTypes.slack:
        return (
          <SlackConfigs
            configs={this.getSectionElements(configSections, AlertTypes.slack)}
            onSave={this.handleSaveConfig(AlertTypes.slack)}
            onTest={this.handleTestConfig(AlertTypes.slack)}
            onDelete={this.handleDeleteConfig(AlertTypes.slack)}
            onEnabled={this.getSpecificConfigEnabled(
              configSections,
              AlertTypes.slack
            )}
          />
        )
      case AlertTypes.smtp:
        return (
          <SMTPConfig
            onSave={this.handleSaveConfig(AlertTypes.smtp)}
            config={this.getSectionElement(configSections, AlertTypes.smtp)}
            onTest={this.handleTestConfig(AlertTypes.smtp)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.smtp)}
          />
        )
      case AlertTypes.talk:
        return (
          <TalkConfig
            onSave={this.handleSaveConfig(AlertTypes.talk)}
            config={this.getSectionElement(configSections, AlertTypes.talk)}
            onTest={this.handleTestConfig(AlertTypes.talk)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.talk)}
          />
        )
      case AlertTypes.telegram:
        return (
          <TelegramConfig
            onSave={this.handleSaveConfig(AlertTypes.telegram)}
            config={this.getSectionElement(configSections, AlertTypes.telegram)}
            onTest={this.handleTestConfig(AlertTypes.telegram)}
            enabled={this.getConfigEnabled(configSections, AlertTypes.telegram)}
          />
        )
      case AlertTypes.victorops:
        return (
          <VictorOpsConfig
            onSave={this.handleSaveConfig(AlertTypes.victorops)}
            config={this.getSectionElement(
              configSections,
              AlertTypes.victorops
            )}
            onTest={this.handleTestConfig(AlertTypes.victorops)}
            enabled={this.getConfigEnabled(
              configSections,
              AlertTypes.victorops
            )}
          />
        )
    }
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
    section: string
  ): Element => {
    return _.get(sections, [section, 'elements', '0'], null)
  }

  private getSectionElements = (
    sections: Sections,
    section: string
  ): Element[] => {
    return _.get(sections, [section, 'elements'], null)
  }

  private getConfigEnabled = (sections: Sections, section: string): boolean => {
    if (section === AlertTypes.slack) {
      const configElements: Section[] = get(sections, `${section}.elements`, [])
      const enabledConfigElements = configElements.filter(e => {
        const enabled: boolean = get(e, 'options.enabled', false)
        return enabled
      })
      return enabledConfigElements.length > 0
    }
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

  private getSpecificConfigEnabled = (sections: Sections, section: string) => (
    specificConfig: string
  ): boolean => {
    const elements: Element[] = this.getSectionElements(sections, section)
    const elementIndex = elements.findIndex(
      element => _.get(element, ['options', 'workspace']) === specificConfig
    )
    return _.get(
      sections,
      [section, 'elements', elementIndex.toString(), 'options', 'enabled'],
      false
    )
  }

  private handleSaveConfig = (section: string) => async (
    properties: ServiceProperties,
    isNewConfigInSection?: boolean,
    specificConfig?: string
  ): Promise<boolean> => {
    if (section !== '') {
      const propsToSend = this.sanitizeProperties(section, properties)
      try {
        if (isNewConfigInSection) {
          await addKapacitorConfigInSection(
            this.props.kapacitor,
            section,
            propsToSend
          )
        } else {
          await updateKapacitorConfigSection(
            this.props.kapacitor,
            section,
            propsToSend,
            specificConfig
          )
        }
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
    e: MouseEvent<HTMLButtonElement>,
    specificConfigOptions?: SpecificConfigOptions
  ): Promise<void> => {
    e.preventDefault()

    try {
      const {data} = await testAlertOutput(
        this.props.kapacitor,
        section,
        options,
        specificConfigOptions
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

  private handleDeleteConfig = (section: string) => async (
    specificConfig: string
  ): Promise<void> => {
    try {
      await deleteKapacitorConfigInSection(
        this.props.kapacitor,
        section,
        specificConfig
      )

      await this.refreshKapacitorConfig(this.props.kapacitor)

      this.props.notify(notifyAlertEndpointDeleted(section, specificConfig))
    } catch (error) {
      const errorMsg = _.join(_.drop(_.split(error, ': '), 2), ': ')
      this.props.notify(
        notifyAlertEndpointDeleteFailed(section, specificConfig, errorMsg)
      )
    }
  }

  private sanitizeProperties = (
    section: string,
    properties: ServiceProperties
  ): ServiceProperties => {
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

  private getInitialIndex = (hash: string): number => {
    const index = _.indexOf(_.keys(SupportedServices), _.replace(hash, '#', ''))
    return index >= 0 ? index : 0
  }

  private isSupportedService = (serviceType: string): boolean => {
    const {services, configSections} = this.state
    const foundKapacitorService: Service = services.find(service => {
      return service.name === serviceType
    })

    const foundSupportedService: string = SupportedServices.find(
      service => service === serviceType
    )

    const foundSection: Section = _.get(configSections, serviceType, undefined)

    const isSupported: boolean =
      !_.isUndefined(foundKapacitorService) &&
      !_.isUndefined(foundSupportedService) &&
      !_.isUndefined(foundSection)

    return isSupported
  }
}

export default AlertTabs
