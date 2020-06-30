// Libraries
import React, {FC} from 'react'
import {FunnelPage, Button, ComponentColor} from '@influxdata/clockface'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Types
import {AppState} from 'src/types'

interface DispatchProps {
  me: AppState['me']
}

type Props = DispatchProps & WithRouterProps

const NoOrgsPage: FC<Props> = ({router, me}) => {
  const handleClick = () => {
    router.push('/signin')
  }

  return (
    <FunnelPage>
      <div className="cf-funnel-page--logo" data-testid="funnel-page--logo">
        <img
          src="https://influxdata.github.io/branding/img/downloads/influxdata-logo--full--white-alpha.png"
          width="170"
        />
      </div>
      <h1 className="cf-funnel-page--title">Whoops!</h1>
      <p className="cf-funnel-page--subtitle">
        You don't belong to an organization.
        <br />
        Add user <strong>{`"${me.name}"`}</strong> to an organization to
        continue
      </p>
      <Button
        text="Sign In"
        color={ComponentColor.Primary}
        onClick={handleClick}
      />
    </FunnelPage>
  )
}

const mstp = ({me}: AppState) => {
  return {me}
}

export default connect(mstp)(withRouter(NoOrgsPage))
