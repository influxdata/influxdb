// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {isEqual} from 'lodash'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'

// Utils
import {AutoRefresher} from 'src/utils/AutoRefresher'

// Actions
import {executeQueries} from 'src/timeMachine/actions/queries'
import {AutoRefreshStatus, AppState} from 'src/types'
import {setAutoRefresh} from 'src/timeMachine/actions'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

class TimeMachineRefreshDropdown extends PureComponent<Props> {
  private autoRefresher = new AutoRefresher()

  public componentDidMount() {
    const {autoRefresh} = this.props
    if (autoRefresh.status === AutoRefreshStatus.Active) {
      this.autoRefresher.poll(autoRefresh.interval)
    }

    this.autoRefresher.subscribe(this.executeQueries)
  }

  public componentDidUpdate(prevProps) {
    const {autoRefresh} = this.props

    if (!isEqual(autoRefresh, prevProps.autoRefresh)) {
      if (autoRefresh.status === AutoRefreshStatus.Active) {
        this.autoRefresher.poll(autoRefresh.interval)
        return
      }

      this.autoRefresher.stopPolling()
    }
  }

  public componentWillUnmount() {
    this.autoRefresher.unsubscribe(this.executeQueries)
    this.autoRefresher.stopPolling()
  }

  public render() {
    const {autoRefresh} = this.props

    return (
      <AutoRefreshDropdown
        selected={autoRefresh}
        onChoose={this.handleChooseAutoRefresh}
        onManualRefresh={this.executeQueries}
      />
    )
  }

  private handleChooseAutoRefresh = (interval: number) => {
    const {onSetAutoRefresh, autoRefresh} = this.props

    if (interval === 0) {
      onSetAutoRefresh({
        ...autoRefresh,
        status: AutoRefreshStatus.Paused,
        interval,
      })
      return
    }

    onSetAutoRefresh({
      ...autoRefresh,
      interval,
      status: AutoRefreshStatus.Active,
    })
  }

  private executeQueries = () => {
    this.props.onExecuteQueries()
  }
}

const mstp = (state: AppState) => {
  const {autoRefresh} = getActiveTimeMachine(state)

  return {autoRefresh}
}

const mdtp = {
  onExecuteQueries: executeQueries,
  onSetAutoRefresh: setAutoRefresh,
}

const connector = connect(mstp, mdtp)

export default connector(TimeMachineRefreshDropdown)
