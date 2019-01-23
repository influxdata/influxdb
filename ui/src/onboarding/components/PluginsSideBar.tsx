import React, {Component} from 'react'

// Components
import SideBar from 'src/onboarding/components/side_bar/SideBar'
import {SideBarTabStatus as TabStatus} from 'src/onboarding/components/side_bar/SideBar'

import {TelegrafPlugin, ConfigurationState} from 'src/types/v2/dataLoaders'

interface Props {
  title: string
  visible: boolean
  telegrafPlugins: TelegrafPlugin[]
  onTabClick: (tabID: string) => void
  currentStepIndex: number
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

class PluginsSideBar extends Component<Props> {
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
      return [...this.tabs]
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
}

export default PluginsSideBar
