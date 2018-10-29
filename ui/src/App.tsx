import React, {SFC, ReactChildren} from 'react'

import Nav from 'src/pageLayout'
import Notifications from 'src/shared/components/notifications/Notifications'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <div className="chronograf-root">
    <Notifications />
    <Nav />
    {children}
  </div>
)

export default App
