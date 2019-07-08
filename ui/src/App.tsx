import React, {SFC, ReactChildren} from 'react'

import RightClickLayer from 'src/clockface/components/right_click_menu/RightClickLayer'
import Nav from 'src/pageLayout'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/containers/Notifications'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <Notifications>
    <RightClickLayer />
    <Nav />
    <TooltipPortal />
    <NotesPortal />
    {children}
  </Notifications>
)

export default App
