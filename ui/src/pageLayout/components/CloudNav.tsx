import React, {PureComponent} from 'react'

// Types
import {IconFont} from 'src/clockface'

export default class CloudNav extends PureComponent {
  render() {
    if (!this.shouldRender) {
      return null
    }

    return (
      <div className="nav--item">
        <a className="nav--item-icon">
          <span className={`icon sidebar--icon ${IconFont.CuboNav}`} />
        </a>
        <div className="nav--item-menu">
          <a className="nav--item-header" href="">
            Cloud
          </a>
          <a className="nav--sub-item" href={this.usageURL}>
            Usage
          </a>
          <a className="nav--sub-item" href={this.billingURL}>
            Billing
          </a>
        </div>
      </div>
    )
  }

  private get shouldRender(): boolean {
    return process.env.CLOUD === 'true'
  }

  private get usageURL(): string {
    return `${process.env.CLOUD_URL}${process.env.CLOUD_USAGE_PATH}`
  }

  private get billingURL(): string {
    return `${process.env.CLOUD_URL}${process.env.CLOUD_BILLING_PATH}`
  }
}
