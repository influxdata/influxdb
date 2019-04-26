// Libraries
import React, {SFC} from 'react'
import {Link} from 'react-router'

// Components
import {Button, ComponentSize} from '@influxdata/clockface'
import CloudExclude from 'src/shared/components/cloud/CloudExclude'
import CloudOnly from 'src/shared/components/cloud/CloudOnly'
import {CLOUD_SIGNOUT_URL} from 'src/shared/constants'

const LogoutButton: SFC = () => (
  <>
    <CloudExclude>
      <Link to="/logout">
        <Button text="Logout" size={ComponentSize.ExtraSmall} />
      </Link>
    </CloudExclude>
    <CloudOnly>
      <a href={CLOUD_SIGNOUT_URL}>
        <Button text="Logout" size={ComponentSize.ExtraSmall} />
      </a>
    </CloudOnly>
  </>
)

export default LogoutButton
