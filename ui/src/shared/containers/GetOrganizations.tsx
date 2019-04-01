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

interface PassedInProps {
  children: React.ReactElement<any>
}

interface DispatchProps {
  getOrganizations: typeof getOrganizationsAction
}

interface StateProps {
  status: RemoteDataState
  org: Organization
}

type Props = StateProps & DispatchProps & PassedInProps

@ErrorHandling
class GetOrganizations extends PureComponent<Props> {
  public async componentDidMount() {
    await this.props.getOrganizations()
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

  return {status, org: items[0]}
}

export default connect<StateProps, DispatchProps, PassedInProps>(
  mstp,
  mdtp
)(GetOrganizations)
