// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router-dom'
import {connect} from 'react-redux'
import {get, find} from 'lodash'

// Components
import {
  Panel,
  ComponentSize,
  Heading,
  HeadingElement,
  Gradients,
  JustifyContent,
  Icon,
  IconFont,
} from '@influxdata/clockface'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {CLOUD_URL, CLOUD_CHECKOUT_PATH} from 'src/shared/constants'
import {
  HIDE_UPGRADE_CTA_KEY,
  PAID_ORG_HIDE_UPGRADE_SETTING,
} from 'src/cloud/constants'

// Types
import {AppState, OrgSetting} from 'src/types'

interface StateProps {
  inView: boolean
}

const CloudUpgradeNavBanner: FC<StateProps> = ({inView}) => {
  return (
    <>
      {inView ? (
        <CloudOnly>
          <Panel
            gradient={Gradients.HotelBreakfast}
            className="cloud-upgrade-banner"
          >
            <Panel.Header
              size={ComponentSize.ExtraSmall}
              justifyContent={JustifyContent.Center}
            >
              <Heading element={HeadingElement.H5}>
                Need more wiggle room?
              </Heading>
            </Panel.Header>
            <Panel.Footer size={ComponentSize.ExtraSmall}>
              <Link
                className="cf-button cf-button-md cf-button-primary cf-button-stretch cloud-upgrade-banner--button"
                to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
                target="_self"
              >
                Upgrade Now
              </Link>
            </Panel.Footer>
          </Panel>
          <Link
            className="cloud-upgrade-banner__collapsed"
            to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
            target="_self"
          >
            <Icon glyph={IconFont.Star} />
            <Heading element={HeadingElement.H5}>Upgrade Now</Heading>
          </Link>
        </CloudOnly>
      ) : null}
    </>
  )
}

const mstp = (state: AppState) => {
  const settings = get(state, 'cloud.orgSettings.settings', [])
  const hideUpgradeButtonSetting = find(
    settings,
    (setting: OrgSetting) => setting.key === HIDE_UPGRADE_CTA_KEY
  )
  if (
    !hideUpgradeButtonSetting ||
    hideUpgradeButtonSetting.value !== PAID_ORG_HIDE_UPGRADE_SETTING.value
  ) {
    return {inView: true}
  }
  return {inView: false}
}

export default connect<StateProps>(mstp)(CloudUpgradeNavBanner)
