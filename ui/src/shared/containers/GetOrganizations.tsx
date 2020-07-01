// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'
import {Route, Switch} from 'react-router-dom'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import NoOrgsPage from 'src/organizations/containers/NoOrgsPage'
import App from 'src/App'

// Types
import {RemoteDataState, AppState} from 'src/types'

// Actions
import {getOrganizations as getOrganizationsAction} from 'src/organizations/actions/thunks'
import RouteToOrg from './RouteToOrg'

interface DispatchProps {
  getOrganizations: typeof getOrganizationsAction
}

interface StateProps {
  status: RemoteDataState
}

type Props = StateProps & DispatchProps

const GetOrganizations: FunctionComponent<Props> = ({
  status,
  getOrganizations,
}) => {
  useEffect(() => {
    if (status === RemoteDataState.NotStarted) {
      getOrganizations()
    }
  }, [])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      <Switch>
        <Route path="/no-orgs" component={NoOrgsPage} />
        <Route path="/orgs" component={App} />
        <Route exact path="/" component={RouteToOrg} />
      </Switch>
    </SpinnerContainer>
  )
}

const mdtp = {
  getOrganizations: getOrganizationsAction,
}

const mstp = ({resources}: AppState): StateProps => ({
  status: resources.orgs.status,
})

export default connect<StateProps, DispatchProps>(mstp, mdtp)(GetOrganizations)
