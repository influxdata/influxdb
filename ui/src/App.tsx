import React, {SFC, ReactChildren} from 'react'

import SideNav from 'src/side_nav'
import Notifications from 'src/shared/components/Notifications'
import Overlay from 'src/shared/components/OverlayTechnology'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <div className="chronograf-root">
    <Overlay />
    <Notifications />
    <SideNav />
    {children}
  </div>
)

export default App
