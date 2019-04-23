// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {getAssetLimits as getAssetLimitsAction} from 'src/cloud/actions/limits'

// Components
import {
  TechnoSpinner,
  SpinnerContainer,
  RemoteDataState,
} from '@influxdata/clockface'
import {AppState} from 'src/types'

interface StateProps {
  status: RemoteDataState
}

interface DispatchProps {
  getAssetLimits: typeof getAssetLimitsAction
}

type Props = StateProps & DispatchProps

class LimitChecker extends PureComponent<Props> {
  public componentDidMount() {
    if (process.env.CLOUD === 'true') {
      this.props.getAssetLimits()
    }
  }

  public render() {
    const {status} = this.props
    if (process.env.CLOUD === 'true') {
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
}: AppState): StateProps => ({status})

const mdtp: DispatchProps = {getAssetLimits: getAssetLimitsAction}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(LimitChecker)
