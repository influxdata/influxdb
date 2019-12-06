import React, {PureComponent} from 'react'

import {SelectGroup, ButtonShape} from '@influxdata/clockface'

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
      <SelectGroup shape={ButtonShape.StretchToFit}>
        {tabs.map(tab => (
          <SelectGroup.Option
            name="bucket-selector"
            key={tab}
            id={tab}
            titleText={tab}
            value={tab}
            active={activeTab === tab}
            onClick={this.handleTabClick}
          >
            {tab}
          </SelectGroup.Option>
        ))}
      </SelectGroup>
    )
  }

  private handleTabClick = (tab: BucketTab) => {
    const {activeTab, onClick} = this.props
    if (tab !== activeTab) {
      onClick(tab)
    }
  }
}
