// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'
import {Organization} from '@influxdata/influx'

// Actions
import {
  getOrganizations as getOrganizationsAction,
  setOrg as setOrgAction,
} from 'src/organizations/actions/orgs'

// Decorators
import {InjectedRouter} from 'react-router'

interface PassedInProps {
  children: React.ReactElement<any>
  router: InjectedRouter
  params: {orgID: string}
}

interface DispatchProps {
  getOrganizations: typeof getOrganizationsAction
  setOrg: typeof setOrgAction
}

interface StateProps {
  status: RemoteDataState
  orgs: Organization[]
}

type Props = StateProps & DispatchProps & PassedInProps

const GetOrganizations: FunctionComponent<Props> = ({
  status,
  params: {orgID},
  orgs,
  router,
  setOrg,
  getOrganizations,
  children,
}) => {
  useEffect(() => {
    if (status === RemoteDataState.NotStarted) {
      getOrganizations()
    }
  })

  useEffect(() => {
    // does orgID from url match any orgs that exist
    const org = orgs.find(o => o.id === orgID)
    const orgExists = !!orgID && !!org
    if (orgExists) {
      setOrg(org)
    }

    if (!orgExists && orgs.length) {
      // default to first org
      router.push(`orgs/${orgs[0].id}`)
    }
  }, [orgID, status])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      {children && React.cloneElement(children)}
    </SpinnerContainer>
  )
}

const mdtp = {
  getOrganizations: getOrganizationsAction,
  setOrg: setOrgAction,
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {status, items},
  } = state

  return {status, orgs: items}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(GetOrganizations)
