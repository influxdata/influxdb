import React, {PureComponent} from 'react'

import {Radio} from 'src/clockface'

import {LineProtocolTab} from 'src/types/v2/dataLoaders'

interface Props {
  active: boolean
  tab: LineProtocolTab
  onClick: (tab: LineProtocolTab) => void
}

export default class extends PureComponent<Props> {
  public render() {
    const {tab, active} = this.props

    return (
      <Radio.Button
        key={tab}
        id={tab}
        titleText={tab}
        value={tab}
        active={active}
        onClick={this.handleClick}
      >
        {tab}
      </Radio.Button>
    )
  }

  private handleClick = () => {
    this.props.onClick(this.props.tab)
  }
}
