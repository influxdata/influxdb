// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getLabels} from 'src/labels/actions'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {LabelsState} from 'src/labels/reducers'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner} from '@influxdata/clockface'

interface StateProps {
  labels: LabelsState
}

interface DispatchProps {
  getLabels: typeof getLabels
}

interface PassedProps {
  resource: ResourceTypes
}

type Props = StateProps & DispatchProps & PassedProps

export enum ResourceTypes {
  Labels = 'labels',
}

@ErrorHandling
class GetResources extends PureComponent<Props, StateProps> {
  public async componentDidMount() {
    switch (this.props.resource) {
      case ResourceTypes.Labels: {
        await this.props.getLabels()
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

const mstp = ({labels}: AppState): StateProps => {
  return {
    labels,
  }
}

const mdtp = {
  getLabels: getLabels,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetResources)
