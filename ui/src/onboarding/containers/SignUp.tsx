import React, {FC} from 'react'

import {
  AlignItems,
  AppWrapper,
  Columns,
  FlexBox,
  FlexDirection,
  Grid,
  JustifyContent,
  Page,
  Panel,
} from '@influxdata/clockface'

import LoginPage from 'src/onboarding/containers/LoginPage'

const SignUp: FC = ({}) => (
  <AppWrapper className="sign-up--page">
    <Page titleTag="Sign Up for InfluxDB Cloud">
      <Page.Contents
        scrollable={true}
        fullWidth={true}
        className="sign-up--page-contents"
      >
        <Panel className="sign-up--panel">
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
                <LoginPage />
              </Grid.Column>
            </Grid.Row>
          </FlexBox>
        </Panel>
      </Page.Contents>
    </Page>
  </AppWrapper>
)

export default SignUp
