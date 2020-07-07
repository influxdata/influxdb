// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {getAssetLimits as getAssetLimitsAction} from 'src/cloud/actions/limits'

// Components
import {
  TechnoSpinner,
  SpinnerContainer,
  RemoteDataState,
} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

// Constants
import {CLOUD} from 'src/shared/constants'

interface StateProps {
  status: RemoteDataState
}

interface DispatchProps {
  getAssetLimits: typeof getAssetLimitsAction
}

type Props = ReduxProps

class GetAssetLimits extends PureComponent<Props> {
  public componentDidMount() {
    if (CLOUD) {
      this.props.getAssetLimits()
    }
  }

  public render() {
    const {status} = this.props
    if (CLOUD) {
      return (
        <SpinnerContainer loading={status} spinnerComponent={<TechnoSpinner />}>
          {this.props.children}
        </SpinnerContainer>
      )
    }
    return this.props.children
  }
}

const mstp = ({
  cloud: {
    limits: {status},
  },
}: AppState) => ({status})

const mdtp = {getAssetLimits: getAssetLimitsAction}

export default connector(GetAssetLimits)
