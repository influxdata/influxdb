// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Actions
import {getAssetLimits as getAssetLimitsAction} from 'src/cloud/actions/limits'

// Components
import {TechnoSpinner, SpinnerContainer} from '@influxdata/clockface'

// Types
import {AppState} from 'src/types'

// Constants
import {CLOUD} from 'src/shared/constants'

interface OwnProps {
  children: React.ReactNode
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

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

const connector = connect(mstp, mdtp)

export default connector(GetAssetLimits)
