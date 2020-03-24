// Libraries
import React, {FC} from 'react'
import {
  AppWrapper,
  FontWeight,
  FunnelPage,
  Heading,
  HeadingElement,
  InfluxDBCloudLogo,
  Typeface,
} from '@influxdata/clockface'

// Components
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import LoginPageContents from 'src/onboarding/containers/LoginPageContents'

export const LoginPage: FC = () => (
  <ErrorBoundary>
    <AppWrapper className="sign-up--page">
      <FunnelPage
        logo={<InfluxDBCloudLogo cloud={true} className="login-page--logo" />}
      >
        <Heading
          element={HeadingElement.H2}
          type={Typeface.Rubik}
          weight={FontWeight.Regular}
          className="heading--margins"
        >
          Create your Free InfluxDB Cloud Account
        </Heading>
        <Heading
          element={HeadingElement.H5}
          type={Typeface.Rubik}
          weight={FontWeight.Regular}
          className="heading--margins"
        >
          No credit card required
        </Heading>
        <LoginPageContents />
      </FunnelPage>
    </AppWrapper>
  </ErrorBoundary>
)
