// Libraries
import React, {useEffect, useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState, Organization} from 'src/types'

// Actions
import {setOrg as setOrgAction} from 'src/organizations/actions/orgs'

// Decorators
import {InjectedRouter} from 'react-router'
import {
  RemoteDataState,
  SpinnerContainer,
  TechnoSpinner,
} from '@influxdata/clockface'

interface PassedInProps {
  children: React.ReactElement<any>
  router: InjectedRouter
  params: {orgID: string}
}

interface DispatchProps {
  setOrg: typeof setOrgAction
}

interface StateProps {
  orgs: Organization[]
}

type Props = StateProps & DispatchProps & PassedInProps

const SetOrg: FunctionComponent<Props> = ({
  params: {orgID},
  orgs,
  router,
  setOrg,
  children,
}) => {
  const [loading, setLoading] = useState(RemoteDataState.Loading)

  useEffect(() => {
    // does orgID from url match any orgs that exist
    const foundOrg = orgs.find(o => o.id === orgID)
    if (foundOrg) {
      setOrg(foundOrg)
      setLoading(RemoteDataState.Done)
      return
    }

    // else default to first org
    router.push(`/orgs/${orgs[0].id}`)
  }, [orgID])

  return (
    <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
      {children}
    </SpinnerContainer>
  )
}

const mdtp = {
  setOrg: setOrgAction,
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {items},
  } = state

  return {orgs: items}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(SetOrg)
