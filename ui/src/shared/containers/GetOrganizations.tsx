// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'

// Actions
import {getOrganizations as getOrganizationsAction} from 'src/organizations/actions/thunks'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface DispatchProps {
  getOrganizations: typeof getOrganizationsAction
}

interface StateProps {
  status: RemoteDataState
}

type Props = StateProps & DispatchProps & PassedInProps

const GetOrganizations: FunctionComponent<Props> = ({
  status,
  getOrganizations,
  children,
}) => {
  useEffect(() => {
    if (status === RemoteDataState.NotStarted) {
      getOrganizations()
    }
  }, [])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      {children && React.cloneElement(children)}
    </SpinnerContainer>
  )
}

const mdtp = {
  getOrganizations: getOrganizationsAction,
}

const mstp = ({resources}: AppState): StateProps => ({
  status: resources.orgs.status,
})

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(GetOrganizations)
