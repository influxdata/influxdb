// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'

// Components
import {
  Panel,
  ComponentSize,
  Heading,
  HeadingElement,
  Gradients,
  JustifyContent,
} from '@influxdata/clockface'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {CLOUD_URL, CLOUD_CHECKOUT_PATH} from 'src/shared/constants'

const CloudUpgradeNavBanner: FC = () => (
  <CloudOnly>
  <Panel gradient={Gradients.HotelBreakfast} className="cloud-upgrade-banner">
    <Panel.Header size={ComponentSize.ExtraSmall} justifyContent={JustifyContent.Center}>
        <Heading element={HeadingElement.H5}>Need more wiggle room?</Heading>
      </Panel.Header>
      <Panel.Footer size={ComponentSize.ExtraSmall}>
        <Link
          className="cf-button cf-button-md cf-button-default cf-button-stretch cloud-upgrade-banner--button"
          to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
        >
          Upgrade Now
        </Link>
      </Panel.Footer>
    </Panel>
  </CloudOnly>
)

export default CloudUpgradeNavBanner
