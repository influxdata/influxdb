import React, {PureComponent} from 'react'
import {SlideToggle, ComponentSize} from 'src/clockface'

interface Props {
  dashboardLink: string
  defaultDashboardLink: string
  onChangeDefault: (link: string) => void
}

class DefaultToggle extends PureComponent<Props> {
  public render() {
    return (
      <SlideToggle
        active={this.isActive}
        onChange={this.handleChange}
        size={ComponentSize.ExtraSmall}
      />
    )
  }

  private handleChange = () => {
    this.props.onChangeDefault(this.props.dashboardLink)
  }

  private get isActive(): boolean {
    const {dashboardLink, defaultDashboardLink} = this.props
    return dashboardLink === defaultDashboardLink
  }
}

export default DefaultToggle
