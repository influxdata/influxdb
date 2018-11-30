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
import {getTelegrafConfigTOML, createTelegrafConfig} from 'src/onboarding/apis'

// Types
import {IconFont} from 'src/clockface'
import {DataSource, ConfigurationState} from 'src/types/v2/dataLoaders'
import {NotificationAction} from 'src/types'

interface Props {
  title: string
  visible: boolean
  dataSources: DataSource[]
  notify: NotificationAction
  onTabClick: (tabID: string) => void
}

const configStateToTabStatus = (cs: ConfigurationState): TabStatus => {
  switch (cs) {
    case ConfigurationState.Unconfigured:
      return TabStatus.Default
    case ConfigurationState.Verifying:
      return TabStatus.Pending
    case ConfigurationState.Configured:
      return TabStatus.Default
    case ConfigurationState.Loading:
      return TabStatus.Pending
    case ConfigurationState.Done:
      return TabStatus.Success
    case ConfigurationState.Error:
      return TabStatus.Error
  }
}

class OnboardingSideBar extends Component<Props> {
  public render() {
    const {title, visible} = this.props
    return (
      <SideBar title={title} visible={visible}>
        {[...this.tabs, ...this.buttons]}
      </SideBar>
    )
  }

  private get tabs(): JSX.Element[] {
    const {dataSources, onTabClick} = this.props
    return dataSources.map(t => (
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

  private get buttons(): JSX.Element[] {
    return [
      <SideBar.Button
        key="Download Config File"
        text="Download Config File"
        titleText="Download Config File"
        color={ComponentColor.Secondary}
        icon={IconFont.Download}
        onClick={this.handleDownload}
      />,
      <SideBar.Button
        text="Add New Source"
        key="Add New Source"
        titleText="Add New Source"
        color={ComponentColor.Default}
        icon={IconFont.Plus}
      />,
    ]
  }

  private handleDownload = async () => {
    const {notify} = this.props
    try {
      const telegraf = await createTelegrafConfig()
      const config = await getTelegrafConfigTOML(telegraf.id)
      downloadTextFile(config, 'config.toml')
    } catch (error) {
      notify(getTelegrafConfigFailed())
    }
  }
}

export default OnboardingSideBar
