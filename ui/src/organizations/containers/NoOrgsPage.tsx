// Libraries
import React, {FC} from 'react'
import {FunnelPage, Button, ComponentColor} from '@influxdata/clockface'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & RouteComponentProps

const NoOrgsPage: FC<Props> = ({history, username}) => {
  const handleClick = () => {
    history.push('/signin')
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
        Add user <strong>{`"${username}"`}</strong> to an organization to
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

const mstp = (state: AppState) => {
  return {
    username: state.me.resource.name,
  }
}

const connector = connect(mstp)

export default connector(withRouter(NoOrgsPage))
