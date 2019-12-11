// Libraries
import React, {FunctionComponent} from 'react'
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
import CloudOnly from 'src/shared/components/cloud/CloudOnly'

// Constants
import {
  CLOUD_URL,
  CLOUD_USAGE_PATH,
  CLOUD_BILLING_PATH,
  CLOUD_SIGNOUT_URL,
} from 'src/shared/constants'

// Types
import {AppState} from 'src/types'

// Images
import Logo from '../images/influxdata-logo.png'

interface StateProps {
  orgName: string
  orgID: string
}

const CloudNav: FunctionComponent<StateProps> = ({orgName, orgID}) => {
  const usageURL = `${CLOUD_URL}${CLOUD_USAGE_PATH}`
  const billingURL = `${CLOUD_URL}${CLOUD_BILLING_PATH}`
  const handleUpgradeClick = (): void => {
    window.location.assign(billingURL)
  }

  return (
    <CloudOnly>
      <AppHeader className="cloud-nav">
        <AppHeader.Logo>
          <Link to={`/orgs/${orgID}`} className="cloud-nav--logo-link">
            <img className="cloud-nav--logo" alt="InfluxData Logo" src={Logo} />
          </Link>
        </AppHeader.Logo>
        <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Medium}>
          <Button
            color={ComponentColor.Success}
            text="Upgrade"
            onClick={handleUpgradeClick}
          />
          <PopNav>
            <p className="cloud-nav--account">
              Logged in as <strong>{orgName}</strong>
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
                <a className={className} href={CLOUD_SIGNOUT_URL}>
                  Logout
                </a>
              )}
            />
          </PopNav>
        </FlexBox>
      </AppHeader>
    </CloudOnly>
  )
}

const mstp = ({orgs: {org}}: AppState) => {
  return {orgName: org.name, orgID: org.id}
}

export default connect<StateProps>(mstp)(CloudNav)
