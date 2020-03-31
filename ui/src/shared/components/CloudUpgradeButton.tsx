// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Components
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {CLOUD_URL, CLOUD_CHECKOUT_PATH} from 'src/shared/constants'

const CloudUpgradeButton: FC = () => {
  return (
    <CloudOnly>
      <FeatureFlag name="treeNav">
        <Link
          className="cf-button cf-button-sm cf-button-success upgrade-payg--button"
          to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
        >
          Upgrade Now
        </Link>
      </FeatureFlag>
    </CloudOnly>
  )
}

export default CloudUpgradeButton
