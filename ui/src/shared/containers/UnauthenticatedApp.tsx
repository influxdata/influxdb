// Libraries
import React, {FC} from 'react'
import {Switch, Route} from 'react-router-dom'

// Components
import {AppWrapper} from '@influxdata/clockface'
import Notifications from 'src/shared/components/notifications/Notifications'
import SigninPage from 'src/onboarding/containers/SigninPage'
import {LoginPage} from 'src/onboarding/containers/LoginPage'
import Logout from 'src/Logout'

// Constants
import {LOGIN, SIGNIN, LOGOUT} from 'src/shared/constants/routes'

const App: FC = () => (
  <AppWrapper>
    <Notifications />
    <Switch>
      <Route path={LOGIN} component={LoginPage} />
      <Route path={SIGNIN} component={SigninPage} />
      <Route path={LOGOUT} component={Logout} />
    </Switch>
  </AppWrapper>
)

export default App
