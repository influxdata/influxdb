import React, {PureComponent} from 'react'

import {Radio, ButtonShape} from '@influxdata/clockface'

import {BucketTab} from 'src/authorizations/utils/permissions'

interface Props {
  tabs: BucketTab[]
  activeTab: BucketTab
  onClick: (tab: string) => void
}

export default class BucketsTabSelector extends PureComponent<Props> {
  public render() {
    const {tabs, activeTab} = this.props
    return (
      <Radio shape={ButtonShape.StretchToFit}>
        {tabs.map(tab => (
          <Radio.Button
            key={tab}
            id={tab}
            titleText={tab}
            value={tab}
            active={activeTab === tab}
            onClick={this.handleTabClick}
          >
            {tab}
          </Radio.Button>
        ))}
      </Radio>
    )
  }

  private handleTabClick = (tab: BucketTab) => {
    const {activeTab, onClick} = this.props
    if (tab !== activeTab) {
      onClick(tab)
    }
  }
}
