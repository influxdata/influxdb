// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Button, ComponentSize} from 'src/clockface'

const LogoutButton: SFC<WithRouterProps> = props => (
  <Link to={`/logout?returnTo=${props.location.pathname}`}>
    <Button text="Logout" size={ComponentSize.ExtraSmall} />
  </Link>
)

export default withRouter<{}>(LogoutButton)
