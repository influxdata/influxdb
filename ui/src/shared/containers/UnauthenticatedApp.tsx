// Libraries
import React, {SFC} from 'react'

// Components
import {AppWrapper} from '@influxdata/clockface'
import Notifications from 'src/shared/components/notifications/Notifications'

const App: SFC<{}> = ({children}) => (
  <AppWrapper>
    <Notifications />
    {children}
  </AppWrapper>
)

export default App
