// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getLabels} from 'src/labels/actions'
import {getBuckets} from 'src/buckets/actions'
import {getTelegrafs} from 'src/telegrafs/actions'

// Types
import {AppState} from 'src/types/v2'
import {LabelsState} from 'src/labels/reducers'
import {BucketsState} from 'src/buckets/reducers'
import {TelegrafsState} from 'src/telegrafs/reducers'
import {Organization} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'
import {getAuthorizations} from 'src/authorizations/actions'
import {AuthorizationsState} from 'src/authorizations/reducers'

interface StateProps {
  org: Organization
  labels: LabelsState
  buckets: BucketsState
  telegrafs: TelegrafsState
  tokens: AuthorizationsState
}

interface DispatchProps {
  getLabels: typeof getLabels
  getBuckets: typeof getBuckets
  getTelegrafs: typeof getTelegrafs
  getAuthorizations: typeof getAuthorizations
}

interface PassedProps {
  resource: ResourceTypes
}

type Props = StateProps & DispatchProps & PassedProps

export enum ResourceTypes {
  Labels = 'labels',
  Buckets = 'buckets',
  Telegrafs = 'telegrafs',
  Authorizations = 'tokens',
}

@ErrorHandling
class GetResources extends PureComponent<Props, StateProps> {
  public async componentDidMount() {
    switch (this.props.resource) {
      case ResourceTypes.Labels: {
        return await this.props.getLabels()
      }

      case ResourceTypes.Buckets: {
        return await this.props.getBuckets()
      }

      case ResourceTypes.Telegrafs: {
        return await this.props.getTelegrafs(this.props.org)
      }

      case ResourceTypes.Authorizations: {
        return await this.props.getAuthorizations()
      }

      default: {
        throw new Error('incorrect resource type provided')
      }
    }
  }

  public render() {
    const {resource, children} = this.props

    return (
      <SpinnerContainer
        loading={this.props[resource].status}
        spinnerComponent={<TechnoSpinner />}
      >
        <>{children}</>
      </SpinnerContainer>
    )
  }
}

const mstp = ({
  orgs,
  labels,
  buckets,
  telegrafs,
  tokens,
}: AppState): StateProps => {
  const org = orgs[0]

  return {
    labels,
    buckets,
    telegrafs,
    tokens,
    org,
  }
}

const mdtp = {
  getLabels: getLabels,
  getBuckets: getBuckets,
  getTelegrafs: getTelegrafs,
  getAuthorizations: getAuthorizations,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetResources)
