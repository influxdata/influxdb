// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

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

// Types
import {AppState, NavBarState} from 'src/types'

interface StateProps {
  navBarState: NavBarState
}

const CloudUpgradeNavBanner: FC<StateProps> = ({navBarState}) => {
  if (navBarState == 'expanded') {
    return (
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
            >
              Upgrade Now
            </Link>
          </Panel.Footer>
        </Panel>
      </CloudOnly>
    )
  }

  return (
    <CloudOnly>
      <Link
        className="cloud-upgrade-banner__collapsed"
        to={`${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`}
      >
        <Icon glyph={IconFont.Star} />
        <Heading element={HeadingElement.H5}>Upgrade Now</Heading>
      </Link>
    </CloudOnly>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    app: {
      persisted: {navBarState},
    },
  } = state

  return {navBarState}
}

export default connect<StateProps, {}>(
  mstp,
  null
)(CloudUpgradeNavBanner)
