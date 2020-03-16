// Libraries
import React, {FC} from 'react'
import {
  AlignItems,
  AppWrapper,
  Columns,
  FlexBox,
  FlexDirection,
  FontWeight,
  Grid,
  Heading,
  HeadingElement,
  JustifyContent,
  Page,
  Panel,
  Typeface,
} from '@influxdata/clockface'

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
          <Panel className="sign-up--panel">
            <Heading
              element={HeadingElement.H2}
              type={Typeface.Rubik}
              weight={FontWeight.Regular}
            >
              Create your Free InfluxDB Cloud Account
            </Heading>
            <FlexBox
              direction={FlexDirection.Column}
              stretchToFitHeight={true}
              justifyContent={JustifyContent.Center}
              alignItems={AlignItems.Center}
            >
              <Grid.Row className="sign-up--full-height">
                <Grid.Column
                  widthXS={Columns.Twelve}
                  widthMD={Columns.Five}
                  offsetMD={Columns.Four}
                  widthLG={Columns.Four}
                  className="sign-up--full-height"
                >
                  <LoginPageContents />
                </Grid.Column>
              </Grid.Row>
            </FlexBox>
          </Panel>
        </Page.Contents>
      </Page>
    </AppWrapper>
  </ErrorBoundary>
)
