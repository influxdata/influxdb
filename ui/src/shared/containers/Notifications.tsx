import React, {SFC} from 'react'

import Notifications from 'src/shared/components/notifications/Notifications'

const App: SFC<{}> = ({children}) => (
  <div className="chronograf-root">
    <Notifications />
    {children}
  </div>
)

export default App
