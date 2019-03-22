// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getLabels} from 'src/labels/actions'
import {getBuckets} from 'src/buckets/actions'
import {getTelegrafs} from 'src/telegrafs/actions'
import {getVariables} from 'src/variables/actions'
import {getScrapers} from 'src/scrapers/actions'

// Types
import {AppState} from 'src/types/v2'
import {LabelsState} from 'src/labels/reducers'
import {BucketsState} from 'src/buckets/reducers'
import {TelegrafsState} from 'src/telegrafs/reducers'
import {Organization} from '@influxdata/influx'
import {ScrapersState} from 'src/scrapers/reducers'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'
import {getAuthorizations} from 'src/authorizations/actions'
import {AuthorizationsState} from 'src/authorizations/reducers'
import {VariablesState} from 'src/variables/reducers'

interface StateProps {
  org: Organization
  labels: LabelsState
  buckets: BucketsState
  telegrafs: TelegrafsState
  variables: VariablesState
  scrapers: ScrapersState
  tokens: AuthorizationsState
}

interface DispatchProps {
  getLabels: typeof getLabels
  getBuckets: typeof getBuckets
  getTelegrafs: typeof getTelegrafs
  getVariables: typeof getVariables
  getScrapers: typeof getScrapers
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
  Variables = 'variables',
  Authorizations = 'tokens',
  Scrapers = 'scrapers',
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
        return await this.props.getTelegrafs()
      }

      case ResourceTypes.Scrapers: {
        return await this.props.getScrapers()
      }

      case ResourceTypes.Variables: {
        return await this.props.getVariables()
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
  variables,
  scrapers,
  tokens,
}: AppState): StateProps => {
  const org = orgs[0]

  return {
    labels,
    buckets,
    telegrafs,
    variables,
    scrapers,
    tokens,
    org,
  }
}

const mdtp = {
  getLabels: getLabels,
  getBuckets: getBuckets,
  getTelegrafs: getTelegrafs,
  getVariables: getVariables,
  getScrapers: getScrapers,
  getAuthorizations: getAuthorizations,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetResources)
