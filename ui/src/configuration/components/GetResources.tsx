// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getLabels} from 'src/labels/actions'
import {getBuckets} from 'src/buckets/actions'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {LabelsState} from 'src/labels/reducers'
import {BucketsState} from 'src/buckets/reducers'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner} from '@influxdata/clockface'

interface StateProps {
  labels: LabelsState
  buckets: BucketsState
}

interface DispatchProps {
  getLabels: typeof getLabels
  getBuckets: typeof getBuckets
}

interface PassedProps {
  resource: ResourceTypes
}

type Props = StateProps & DispatchProps & PassedProps

export enum ResourceTypes {
  Labels = 'labels',
  Buckets = 'buckets',
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
    }
  }

  public render() {
    const {resource, children} = this.props

    if (this.props[resource].status != RemoteDataState.Done) {
      return <TechnoSpinner />
    }

    return children
  }
}

const mstp = ({labels, buckets}: AppState): StateProps => {
  return {
    labels,
    buckets,
  }
}

const mdtp = {
  getLabels: getLabels,
  getBuckets: getBuckets,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetResources)
