// Libraries
import React, {FC} from 'react'
import {
  AppWrapper,
  FontWeight,
  FunnelPage,
  Heading,
  HeadingElement,
  InfluxColors,
  InfluxDBCloudLogo,
  Typeface,
} from '@influxdata/clockface'

// Components
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import LoginPageContents from 'src/onboarding/containers/LoginPageContents'

export const LoginPage: FC = () => (
  <ErrorBoundary>
    <AppWrapper>
      <FunnelPage
        accentColorA={InfluxColors.Magenta}
        accentColorB={InfluxColors.Amethyst}
        backgroundColor={InfluxColors.DeepPurple}
        enableGraphic={true}
        logo={<InfluxDBCloudLogo cloud={true} className="login-page--logo" />}
      >
        <Heading
          element={HeadingElement.H1}
          type={Typeface.Rubik}
          weight={FontWeight.Regular}
          className="cf-funnel-page--title"
        >
          Log in to your InfluxDB Cloud Account
        </Heading>
        <LoginPageContents />
      </FunnelPage>
    </AppWrapper>
  </ErrorBoundary>
)
