// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'
import {CLOUD} from 'src/shared/constants'

// Actions
import {getReadWriteCardinalityLimits as getReadWriteCardinalityLimitsAction} from 'src/cloud/actions/limits'

interface DispatchProps {
  getReadWriteCardinalityLimits: typeof getReadWriteCardinalityLimitsAction
}

class LimitChecker extends PureComponent<DispatchProps, {}> {
  public componentDidMount() {
    if (CLOUD) {
      this.props.getReadWriteCardinalityLimits()
    }
  }

  public render() {
    return this.props.children
  }
}

const mdtp: DispatchProps = {
  getReadWriteCardinalityLimits: getReadWriteCardinalityLimitsAction,
}

export default connect<{}, DispatchProps, {}>(null, mdtp)(LimitChecker)
