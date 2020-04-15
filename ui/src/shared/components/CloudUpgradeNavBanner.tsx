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
  Icon,
  IconFont,
} from '@influxdata/clockface'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {CLOUD_URL, CLOUD_CHECKOUT_PATH} from 'src/shared/constants'

const CloudUpgradeNavBanner: FC = () => {
  return (
    <>
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
    </>
  )
}

export default CloudUpgradeNavBanner
