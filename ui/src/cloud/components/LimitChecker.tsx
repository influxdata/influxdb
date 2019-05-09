// Libraries
import {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {getReadWriteLimits as getReadWriteLimitsAction} from 'src/cloud/actions/limits'

interface DispatchProps {
  getReadWriteLimits: typeof getReadWriteLimitsAction
}

class LimitChecker extends PureComponent<DispatchProps, {}> {
  public componentDidMount() {
    if (process.env.CLOUD === 'true') {
      this.props.getReadWriteLimits()
    }
  }

  public render() {
    return this.props.children
  }
}

const mdtp: DispatchProps = {getReadWriteLimits: getReadWriteLimitsAction}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(LimitChecker)
