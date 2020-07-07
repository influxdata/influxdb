// Libraries
import {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {CLOUD} from 'src/shared/constants'

// Actions
import {getReadWriteCardinalityLimits as getReadWriteCardinalityLimitsAction} from 'src/cloud/actions/limits'

interface OwnProps {
  children: React.ReactNode
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

class LimitChecker extends PureComponent<Props> {
  public componentDidMount() {
    if (CLOUD) {
      this.props.getReadWriteCardinalityLimits()
    }
  }

  public render() {
    return this.props.children
  }
}

const mdtp = {
  getReadWriteCardinalityLimits: getReadWriteCardinalityLimitsAction,
}

const connector = connect(null, mdtp)

export default connector(LimitChecker)
