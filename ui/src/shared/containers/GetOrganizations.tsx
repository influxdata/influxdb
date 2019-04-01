// Libraries
import React, {PureComponent} from 'react'
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
import {ErrorHandling} from 'src/shared/decorators/errors'
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

@ErrorHandling
class GetOrganizations extends PureComponent<Props> {
  public componentDidMount() {
    const {getOrganizations} = this.props

    getOrganizations()
  }

  public componentDidUpdate() {
    const {
      router,
      params: {orgID},
      orgs,
      setOrg,
    } = this.props

    //does orgID from url match any orgs that exist
    const org = orgs.find(o => o.id === orgID)
    const orgExists = !!orgID && !!org
    if (orgExists) {
      setOrg(org)
    }

    if (!orgExists && orgs.length) {
      //default to first org
      router.push(`org/${orgs[0].id}`)
      setOrg(orgs[0])
    }
  }

  public render() {
    const {status} = this.props

    return (
      <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
        {this.props.children && React.cloneElement(this.props.children)}
      </SpinnerContainer>
    )
  }
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
