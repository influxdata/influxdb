// Libraries
import React, {useEffect, useState, FC} from 'react'
import {connect} from 'react-redux'

// Types
import {AppState, Organization, ResourceType} from 'src/types'

// Actions
import {setOrg as setOrgAction} from 'src/organizations/actions/creators'

// Utils
import {updateReportingContext} from 'src/cloud/utils/reporting'

// Decorators
import {InjectedRouter} from 'react-router-dom'
import {
  RemoteDataState,
  SpinnerContainer,
  TechnoSpinner,
} from '@influxdata/clockface'

// Selectors
import {getAll} from 'src/resources/selectors'

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

const SetOrg: FC<Props> = ({
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
      updateReportingContext({orgID: orgID})
      setLoading(RemoteDataState.Done)
      return
    }
    updateReportingContext({orgID: null})

    if (!orgs.length) {
      router.push(`/no-orgs`)
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
  const orgs = getAll<Organization>(state, ResourceType.Orgs)

  return {orgs}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(SetOrg)
