import React, {Component} from 'react'

// Components
import SideBar from 'src/onboarding/components/side_bar/SideBar'
import {SideBarTabStatus as TabStatus} from 'src/onboarding/components/side_bar/SideBar'
import {ComponentColor} from 'src/clockface'

// Utils
import {downloadTextFile} from 'src/shared/utils/download'

// Constants
import {getTelegrafConfigFailed} from 'src/shared/copy/v2/notifications'

// APIs
import {getTelegrafConfigTOML} from 'src/onboarding/apis'

// Types
import {IconFont} from 'src/clockface'
import {TelegrafPlugin, ConfigurationState} from 'src/types/v2/dataLoaders'
import {NotificationAction} from 'src/types'

interface Props {
  title: string
  telegrafConfigID: string
  visible: boolean
  telegrafPlugins: TelegrafPlugin[]
  notify: NotificationAction
  onTabClick: (tabID: string) => void
  currentStepIndex: number
  onNewSourceClick: () => void
}

const configStateToTabStatus = (cs: ConfigurationState): TabStatus => {
  switch (cs) {
    case ConfigurationState.Unconfigured:
      return TabStatus.Default
    case ConfigurationState.InvalidConfiguration:
      return TabStatus.Error
    case ConfigurationState.Configured:
      return TabStatus.Success
  }
}

class OnboardingSideBar extends Component<Props> {
  public render() {
    const {title, visible} = this.props
    return (
      <SideBar title={title} visible={visible}>
        {this.content}
      </SideBar>
    )
  }
  private get content(): JSX.Element[] {
    const {currentStepIndex} = this.props
    if (currentStepIndex !== 2) {
      return [...this.tabs, ...this.buttons]
    }
    return this.tabs
  }

  private get tabs(): JSX.Element[] {
    const {telegrafPlugins, onTabClick} = this.props
    return telegrafPlugins.map(t => (
      <SideBar.Tab
        label={t.name}
        key={t.name}
        id={t.name}
        active={t.active}
        status={configStateToTabStatus(t.configured)}
        onClick={onTabClick}
      />
    ))
  }

  private get downloadButton(): JSX.Element {
    return (
      <SideBar.Button
        key="Download Config File"
        text="Download Config File"
        titleText="Download Config File"
        data-test="download"
        color={ComponentColor.Secondary}
        icon={IconFont.Download}
        onClick={this.handleDownload}
      />
    )
  }

  private get addSourceButton(): JSX.Element {
    const {onNewSourceClick} = this.props

    return (
      <SideBar.Button
        text="Add New Source"
        key="Add New Source"
        titleText="Add New Source"
        data-test="new"
        color={ComponentColor.Default}
        icon={IconFont.Plus}
        onClick={onNewSourceClick}
      />
    )
  }

  private get buttons(): JSX.Element[] {
    const {telegrafConfigID} = this.props

    if (telegrafConfigID) {
      return [this.downloadButton, this.addSourceButton]
    }
    return [this.addSourceButton]
  }

  private handleDownload = async () => {
    const {notify, telegrafConfigID} = this.props
    try {
      const config = await getTelegrafConfigTOML(telegrafConfigID)
      downloadTextFile(config, 'config.toml')
    } catch (error) {
      notify(getTelegrafConfigFailed())
    }
  }
}

export default OnboardingSideBar
