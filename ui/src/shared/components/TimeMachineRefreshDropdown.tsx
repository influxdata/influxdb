// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'

// Utils
import {AutoRefresher} from 'src/utils/AutoRefresher'

// Constants
import {FIVE_SECONDS} from 'src/shared/constants'

// Types
import {incrementSubmitToken} from 'src/shared/actions/v2/timeMachines'

interface DispatchProps {
  onIncrementSubmitToken: typeof incrementSubmitToken
}

interface State {
  autoRefreshInterval: number
}

class TimeMachineRefreshDropdown extends PureComponent<DispatchProps, State> {
  public state: State = {autoRefreshInterval: FIVE_SECONDS}
  private autoRefresher = new AutoRefresher()

  public componentDidMount() {
    const {autoRefreshInterval} = this.state

    this.autoRefresher.poll(autoRefreshInterval)
    this.autoRefresher.subscribe(this.incrementSubmitToken)
  }

  public componentDidUpdate(__, prevState) {
    const {autoRefreshInterval} = this.state

    if (autoRefreshInterval !== prevState.autoRefreshInterval) {
      this.autoRefresher.poll(autoRefreshInterval)
    }
  }

  public componentWillUnmount() {
    this.autoRefresher.unsubscribe(this.incrementSubmitToken)
    this.autoRefresher.stopPolling()
  }

  public render() {
    const {autoRefreshInterval} = this.state

    return (
      <AutoRefreshDropdown
        selected={autoRefreshInterval}
        onChoose={this.handleChooseInterval}
      />
    )
  }

  private handleChooseInterval = (autoRefreshInterval: number) => {
    this.setState({autoRefreshInterval})
  }

  private incrementSubmitToken = () => {
    const {onIncrementSubmitToken} = this.props

    onIncrementSubmitToken()
  }
}

const mdtp: DispatchProps = {
  onIncrementSubmitToken: incrementSubmitToken,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(TimeMachineRefreshDropdown)
