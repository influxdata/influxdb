// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Actions
import {getLabels} from 'src/labels/actions'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {TechnoSpinner} from '@influxdata/clockface'

interface StateProps {
  status: RemoteDataState
}

interface DispatchProps {
  getLabels: typeof getLabels
}

type Props = StateProps & DispatchProps

@ErrorHandling
class GetLabels extends PureComponent<Props, StateProps> {
  public async componentDidMount() {
    await this.props.getLabels()
  }

  public render() {
    const {status, children} = this.props

    if (status != RemoteDataState.Done) {
      return <TechnoSpinner />
    }

    return children
  }
}

const mstp = ({labels}: AppState): StateProps => {
  return {
    status: labels.status,
  }
}

const mdtp = {
  getLabels: getLabels,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(GetLabels)
