import React, {PureComponent} from 'react'

import {SelectGroup} from '@influxdata/clockface'

import {LineProtocolTab} from 'src/types'

interface Props {
  active: boolean
  tab: LineProtocolTab
  onClick: (tab: LineProtocolTab) => void
}

export default class extends PureComponent<Props> {
  public render() {
    const {tab, active} = this.props

    return (
      <SelectGroup.Option
        name="line-protocol"
        key={tab}
        id={tab}
        titleText={tab}
        value={tab}
        active={active}
        onClick={this.handleClick}
        testID={tab}
      >
        {tab}
      </SelectGroup.Option>
    )
  }

  private handleClick = () => {
    this.props.onClick(this.props.tab)
  }
}
