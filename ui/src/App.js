import React from 'react'
import PropTypes from 'prop-types'

import SideNav from 'src/side_nav'
import Notifications from 'shared/components/Notifications'
import Overlay from 'shared/components/OverlayTechnology'

const App = ({children}) => (
  <div className="chronograf-root">
    <Overlay />
    <Notifications />
    <SideNav />
    {children}
  </div>
)

const {node} = PropTypes

App.propTypes = {
  children: node.isRequired,
}

export default App
