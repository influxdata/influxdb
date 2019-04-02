// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'
import {Organization} from '@influxdata/influx'

// Actions
import {setOrg as setOrgAction} from 'src/organizations/actions/orgs'

// Decorators
import {InjectedRouter} from 'react-router'

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
  status: RemoteDataState
}

type Props = StateProps & DispatchProps & PassedInProps

const SetOrg: FunctionComponent<Props> = ({
  params: {orgID},
  orgs,
  router,
  setOrg,
  children,
  status,
}) => {
  useEffect(() => {
    // does orgID from url match any orgs that exist
    const foundOrg = orgs.find(o => o.id === orgID)
    const orgExists = !!orgID && !!foundOrg
    if (orgExists) {
      setOrg(foundOrg)
      return
    }

    // else default to first org
    router.push(`orgs/${orgs[0].id}`)
  }, [orgID])

  return (
    <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
      {children && React.cloneElement(children)}
    </SpinnerContainer>
  )
}

const mdtp = {
  setOrg: setOrgAction,
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {items, org},
  } = state

  const status = org ? RemoteDataState.Done : RemoteDataState.Loading

  return {orgs: items, status}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(SetOrg)
