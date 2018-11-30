// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Actions
import {
  setActiveQueryIndex,
  removeQuery,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueryTab.scss'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  activeQueryIndex: number
}

interface DispatchProps {
  onSetActiveQueryIndex: typeof setActiveQueryIndex
  onRemoveQuery: typeof removeQuery
}

interface OwnProps {
  queryIndex: number
}

type Props = StateProps & DispatchProps & OwnProps

class TimeMachineQueryTab extends PureComponent<Props> {
  public render() {
    const {queryIndex, activeQueryIndex} = this.props
    const activeClass = queryIndex === activeQueryIndex ? 'active' : ''

    return (
      <div
        className={`time-machine-query-tab ${activeClass}`}
        onClick={this.handleSetActive}
      >
        Query {queryIndex + 1}
        <div
          className="time-machine-query-tab--close"
          onClick={this.handleRemove}
        >
          <span className="icon remove" />
        </div>
      </div>
    )
  }

  private handleSetActive = (): void => {
    const {queryIndex, onSetActiveQueryIndex} = this.props

    onSetActiveQueryIndex(queryIndex)
  }

  private handleRemove = (e: MouseEvent<HTMLDivElement>): void => {
    const {queryIndex, onRemoveQuery} = this.props

    e.stopPropagation()
    onRemoveQuery(queryIndex)
  }
}

const mstp = (state: AppState) => {
  const {activeQueryIndex} = getActiveTimeMachine(state)
  return {activeQueryIndex}
}

const mdtp = {
  onSetActiveQueryIndex: setActiveQueryIndex,
  onRemoveQuery: removeQuery,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueryTab)
