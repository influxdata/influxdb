// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
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
  show: boolean
}

const CloudUpgradeButton: FC<StateProps> = ({show}) => {
  return (
    <CloudOnly>
      {show ? (
        <Link
          className="cf-button cf-button-sm cf-button-success upgrade-payg--button"
          to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
          target="_self"
        >
          Upgrade Now
        </Link>
      ) : null}
    </CloudOnly>
  )
}

const mstp = ({
  cloud: {
    orgSettings: {settings},
  },
}: AppState) => {
  const hideUpgradeCTA = settings.find(
    (setting: OrgSetting) => setting.key === HIDE_UPGRADE_CTA_KEY
  )
  if (
    !hideUpgradeCTA ||
    hideUpgradeCTA.value !== PAID_ORG_HIDE_UPGRADE_SETTING.value
  ) {
    return {show: true}
  }
  return {show: false}
}

export default connect<StateProps>(mstp)(CloudUpgradeButton)
