import React, {PureComponent} from 'react'

// Components
import {NavMenu, Icon} from '@influxdata/clockface'

// Types
import {IconFont} from '@influxdata/clockface'

export default class CloudNav extends PureComponent {
  render() {
    if (!this.shouldRender) {
      return null
    }

    return (
      <NavMenu.Item
        active={false}
        titleLink={className => (
          <a className={className} href={this.usageURL}>
            Usage
          </a>
        )}
        iconLink={className => (
          <a className={className} href={this.usageURL}>
            <Icon glyph={IconFont.Cloud} />
          </a>
        )}
      >
        <NavMenu.SubItem
          active={false}
          titleLink={className => (
            <a className={className} href={this.usageURL}>
              Usage
            </a>
          )}
        />
        <NavMenu.SubItem
          active={false}
          titleLink={className => (
            <a className={className} href={this.billingURL}>
              Billing
            </a>
          )}
        />
      </NavMenu.Item>
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
