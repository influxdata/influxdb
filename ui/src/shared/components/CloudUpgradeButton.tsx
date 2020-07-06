// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router-dom'
import {connect} from 'react-redux'
import {get, find} from 'lodash'

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
  inView: boolean
}

const CloudUpgradeButton: FC<StateProps> = ({inView}) => {
  return (
    <CloudOnly>
      {inView ? (
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

export default connect<StateProps>(mstp)(CloudUpgradeButton)
