import React, {SFC, ReactChildren} from 'react'

import RightClickLayer from 'src/clockface/components/right_click_menu/RightClickLayer'
import Nav from 'src/pageLayout'
import LegendPortal from 'src/shared/components/LegendPortal'
import TooltipPortal from 'src/shared/components/TooltipPortal'
import Notifications from 'src/shared/containers/Notifications'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <Notifications>
    <RightClickLayer />
    <Nav />
    <LegendPortal />
    <TooltipPortal />
    {children}
  </Notifications>
)

export default App
