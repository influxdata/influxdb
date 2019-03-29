// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState} from 'src/types'
import {Organization} from '@influxdata/influx'

// Actions
import {getOrganizations as getOrganizationsAction} from 'src/organizations/actions/orgs'

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
    } = this.props

    const org = orgs.find(o => o.id === orgID)

    const orgExists = !!orgID && !!org

    if (!orgExists && orgs.length) {
      router.push(`orgs/${orgs[0].id}`)
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
