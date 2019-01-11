import React, {PureComponent} from 'react'

import {Radio, ButtonShape} from 'src/clockface'
import {LineProtocolTab} from 'src/types/v2/dataLoaders'

import Tab from 'src/onboarding/components/configureStep/lineProtocol/Tab'

interface Props {
  tabs: LineProtocolTab[]
  activeLPTab: LineProtocolTab
  onClick: (tab: LineProtocolTab) => void
}

export default class extends PureComponent<Props> {
  public render() {
    const {tabs, activeLPTab} = this.props
    return (
      <Radio shape={ButtonShape.Default}>
        {tabs.map(t => (
          <Tab
            tab={t}
            key={t}
            active={activeLPTab === t}
            onClick={this.handleTabClick}
          />
        ))}
      </Radio>
    )
  }

  private handleTabClick = (tab: LineProtocolTab) => {
    const {activeLPTab, onClick} = this.props
    if (tab !== activeLPTab) {
      onClick(tab)
    }
  }
}
