// Libraries
import React, {FC} from 'react'
import {FunnelPage, Button} from '@influxdata/clockface'
import {withRouter, WithRouterProps} from 'react-router'

const NoOrgsPage: FC<WithRouterProps> = ({router}) => {
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
      <h1 className="cf-funnel-page--title">Whoops, no orgs!</h1>
      <p>Add this user to an organization to continue</p>
      <Button text="Sign In" onClick={handleClick} />
    </FunnelPage>
  )
}

export default withRouter(NoOrgsPage)
