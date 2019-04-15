// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'

// Components
import {Button, ComponentSize} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudInclude from 'src/shared/components/cloud/CloudExclude'
import {CLOUD_SIGNOUT_URL} from 'src/shared/constants'

const LogoutButton: SFC = () => (
  <>
    <CloudExclude>
      <Link to="/logout">
        <Button text="Logout" size={ComponentSize.ExtraSmall} />
      </Link>
    </CloudExclude>
    <CloudInclude>
      <a href={CLOUD_SIGNOUT_URL}>
        <Button text="Logout" size={ComponentSize.ExtraSmall} />
      </a>
    </CloudInclude>
  </>
)

export default LogoutButton
