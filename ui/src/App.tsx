import React, {SFC, ReactChildren} from 'react'

import {AppWrapper} from '@influxdata/clockface'
import RightClickLayer from 'src/clockface/components/right_click_menu/RightClickLayer'
import Nav from 'src/pageLayout'
import TooltipPortal from 'src/portals/TooltipPortal'
import NotesPortal from 'src/portals/NotesPortal'
import Notifications from 'src/shared/components/notifications/Notifications'

interface Props {
  children: ReactChildren
}

const App: SFC<Props> = ({children}) => (
  <AppWrapper>
    <Notifications />
    <RightClickLayer />
    <TooltipPortal />
    <NotesPortal />
    <Nav />
    {children}
  </AppWrapper>
)

export default App
