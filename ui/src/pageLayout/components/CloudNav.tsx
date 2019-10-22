import React, {PureComponent} from 'react'

// Components
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import {NavMenu, Icon} from '@influxdata/clockface'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {
  CLOUD_URL,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
} from 'src/shared/constants'

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
    return `${CLOUD_URL}${CLOUD_USAGE_PATH}`
  }

  private get billingURL(): string {
    return `${CLOUD_URL}${CLOUD_BILLING_PATH}`
  }
}
