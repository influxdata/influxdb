import React, {PureComponent} from 'react'

// Components
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import {NavMenu, Icon} from '@influxdata/clockface'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Types
import {IconFont} from '@influxdata/clockface'

export default class CloudNav extends PureComponent {
  render() {
    return (
      <CloudOnly>
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
          <FeatureFlag name="cloudBilling">
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
          </FeatureFlag>
        </NavMenu.Item>
      </CloudOnly>
    )
  }

  private get usageURL(): string {
    return `${process.env.CLOUD_URL}${process.env.CLOUD_USAGE_PATH}`
  }

  private get billingURL(): string {
    return `${process.env.CLOUD_URL}${process.env.CLOUD_BILLING_PATH}`
  }
}
