// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SplashPage from 'src/shared/components/splash_page/SplashPage'
import SigninForm from 'src/onboarding/components/SigninForm'

@ErrorHandling
class SigninPage extends PureComponent<{}> {
  public render() {
    return (
      <SplashPage panelWidthPixels={300}>
        <SplashPage.Panel>
          <SplashPage.Logo />
          <SplashPage.Header title="InfluxData" />
          <SigninForm />
        </SplashPage.Panel>
      </SplashPage>
    )
  }
}

export default SigninPage
