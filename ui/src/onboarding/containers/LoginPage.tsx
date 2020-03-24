// Libraries
import React, {FC} from 'react'
import {AppWrapper, FunnelPage, InfluxDBCloudLogo} from '@influxdata/clockface'

// Components
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import LoginPageContents from 'src/onboarding/containers/LoginPageContents'

export const LoginPage: FC = () => (
  <ErrorBoundary>
    <AppWrapper className="sign-up--page">
      <FunnelPage
        logo={<InfluxDBCloudLogo cloud={true} className="login-page--logo" />}
      >
        <h2 className="cf-funnel-page--title">
          Create your Free InfluxDB Cloud Account
        </h2>
        <h5 className="cf-funnel-page--subtitle">No credit card required</h5>
        <LoginPageContents />
      </FunnelPage>
    </AppWrapper>
  </ErrorBoundary>
)
