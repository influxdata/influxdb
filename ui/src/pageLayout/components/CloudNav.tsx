// Libraries
import React, {FC} from 'react'
import {Link} from 'react-router'
import {connect} from 'react-redux'

// Components
import {FeatureFlag} from 'src/shared/utils/featureFlag'
import {
  AppHeader,
  PopNav,
  Button,
  ComponentColor,
  FlexBox,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'

// Constants
import {
  CLOUD_URL,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
  CLOUD_CHECKOUT_PATH,
  CLOUD_LOGOUT_PATH,
} from 'src/shared/constants'

// Types
import {AppState, Organization} from 'src/types'
import {MeState} from 'src/shared/reducers/me'

// Images

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  org: Organization
  me: MeState
}

const Logo = require('../images/influxdata-logo.png')

const CloudNav: FC<StateProps> = ({org, me}) => {
  const usageURL = `${CLOUD_URL}${CLOUD_USAGE_PATH}`
  const billingURL = `${CLOUD_URL}${CLOUD_BILLING_PATH}`
  const checkoutURL = `${CLOUD_URL}${CLOUD_CHECKOUT_PATH}`
  const logoutURL = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`
  const handleUpgradeClick = () => {
    window.location.assign(checkoutURL)
  }

  if (!org) {
    return (
      <AppHeader className="cloud-nav">
        <AppHeader.Logo>
          <img className="cloud-nav--logo" alt="InfluxData Logo" src={Logo} />
        </AppHeader.Logo>
      </AppHeader>
    )
  }

  return (
    <AppHeader className="cloud-nav">
      <AppHeader.Logo>
        <Link to={`/orgs/${org.id}`} className="cloud-nav--logo-link">
          <img className="cloud-nav--logo" alt="InfluxData Logo" src={Logo} />
        </Link>
      </AppHeader.Logo>
      <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Medium}>
        <Button
          color={ComponentColor.Success}
          text="Upgrade Now"
          onClick={handleUpgradeClick}
          className="upgrade-payg--button"
        />
        <PopNav>
          <p className="cloud-nav--account">
            Logged in as <strong>{me.name}</strong>
          </p>
          <PopNav.Item
            active={false}
            titleLink={className => (
              <a className={className} href={usageURL}>
                Usage
              </a>
            )}
          />
          <FeatureFlag name="cloudBilling">
            <PopNav.Item
              active={false}
              titleLink={className => (
                <a className={className} href={billingURL}>
                  Billing
                </a>
              )}
            />
          </FeatureFlag>
          <PopNav.Item
            active={false}
            titleLink={className => (
              <a className={className} href={logoutURL}>
                Logout
              </a>
            )}
          />
        </PopNav>
      </FlexBox>
    </AppHeader>
  )
}

const mstp = (state: AppState) => {
  const org = getOrg(state)
  const me = state.me
  return {org, me}
}

export default connect<StateProps>(mstp)(CloudNav)
