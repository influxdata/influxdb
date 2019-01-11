import React, {SFC, ReactChildren} from 'react'

import RightClickLayer from 'src/clockface/components/right_click_menu/RightClickLayer'
import Nav from 'src/pageLayout'
import Notifications from 'src/shared/components/notifications/Notifications'
import LegendPortal from 'src/shared/components/LegendPortal'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <div className="chronograf-root">
    <Notifications />
    <RightClickLayer />
    <Nav />
    <LegendPortal />
    {children}
  </div>
)

export default App
