import React, {PureComponent} from 'react'

import {SelectGroup, ButtonShape} from '@influxdata/clockface'
import {LineProtocolTab} from 'src/types'

import Tab from 'src/buckets/components/lineProtocol/configure/Tab'

interface Props {
  tabs: LineProtocolTab[]
  activeLPTab: LineProtocolTab
  onClick: (tab: LineProtocolTab) => void
}

export default class extends PureComponent<Props> {
  public render() {
    const {tabs, activeLPTab} = this.props
    return (
      <SelectGroup shape={ButtonShape.Default}>
        {tabs.map(t => (
          <Tab
            tab={t}
            key={t}
            active={activeLPTab === t}
            onClick={this.handleTabClick}
          />
        ))}
      </SelectGroup>
    )
  }

  private handleTabClick = (tab: LineProtocolTab) => {
    const {activeLPTab, onClick} = this.props
    if (tab !== activeLPTab) {
      onClick(tab)
    }
  }
}
