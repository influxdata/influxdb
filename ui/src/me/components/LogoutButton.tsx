// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'

// Components
import {Button, ComponentSize} from '@influxdata/clockface'

const LogoutButton: SFC = () => (
  <>
    <Link to="/logout">
      <Button text="Logout" size={ComponentSize.ExtraSmall} />
    </Link>
  </>
)

export default LogoutButton
