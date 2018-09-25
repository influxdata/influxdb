import React, {PureComponent} from 'react'
import SlideToggle from 'src/reusable_ui/components/slide_toggle/SlideToggle'

interface Props {
  dashboardLink: string
  defaultDashboardLink: string
  onChangeDefault: (link: string) => void
}

class DefaultToggle extends PureComponent<Props> {
  public render() {
    return <SlideToggle active={this.isActive} onChange={this.handleChange} />
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
