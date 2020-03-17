// Libraries
import React, {FC} from 'react'
import {AppWrapper, Page} from '@influxdata/clockface'

// Components
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import LoginPageContents from 'src/onboarding/containers/LoginPageContents'

export const LoginPage: FC = () => (
  <ErrorBoundary>
    <AppWrapper className="sign-up--page">
      <Page titleTag="Sign Up for InfluxDB Cloud">
        <Page.Contents
          scrollable={true}
          fullWidth={true}
          className="sign-up--page-contents"
        >
          <h2 className="cf-funnel-page--title">
            Create your Free InfluxDB Cloud Account
          </h2>
          <h5 className="cf-funnel-page--subtitle">No credit card required</h5>
          <LoginPageContents />
        </Page.Contents>
      </Page>
    </AppWrapper>
  </ErrorBoundary>
)
