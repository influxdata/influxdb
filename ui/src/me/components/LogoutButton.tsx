// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router-dom'

// Components
import {Button, ComponentSize} from '@influxdata/clockface'

const LogoutButton: SFC = () => (
  <>
    <Link to="/logout">
      <Button
        text="Logout"
        size={ComponentSize.ExtraSmall}
        testID="logout--button"
      />
    </Link>
  </>
)

export default LogoutButton
