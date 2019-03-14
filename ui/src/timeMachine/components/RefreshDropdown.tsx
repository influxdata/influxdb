// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'

// Utils
import {AutoRefresher} from 'src/utils/AutoRefresher'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'

interface DispatchProps {
  onExecuteQueries: typeof executeQueries
}

interface State {
  autoRefreshInterval: number
}

class TimeMachineRefreshDropdown extends PureComponent<DispatchProps, State> {
  public state: State = {autoRefreshInterval: 0}
  private autoRefresher = new AutoRefresher()

  public componentDidMount() {
    const {autoRefreshInterval} = this.state

    this.autoRefresher.poll(autoRefreshInterval)
    this.autoRefresher.subscribe(this.executeQueries)
  }

  public componentDidUpdate(__, prevState) {
    const {autoRefreshInterval} = this.state

    if (autoRefreshInterval !== prevState.autoRefreshInterval) {
      this.autoRefresher.poll(autoRefreshInterval)
    }
  }

  public componentWillUnmount() {
    this.autoRefresher.unsubscribe(this.executeQueries)
    this.autoRefresher.stopPolling()
  }

  public render() {
    const {autoRefreshInterval} = this.state

    return (
      <AutoRefreshDropdown
        selected={autoRefreshInterval}
        onChoose={this.handleChooseInterval}
        onManualRefresh={this.executeQueries}
      />
    )
  }

  private handleChooseInterval = (autoRefreshInterval: number) => {
    this.setState({autoRefreshInterval})
  }

  private executeQueries = () => {
    this.props.onExecuteQueries()
  }
}

const mdtp: DispatchProps = {
  onExecuteQueries: executeQueries,
}

export default connect<{}, DispatchProps>(
  null,
  mdtp
)(TimeMachineRefreshDropdown)
