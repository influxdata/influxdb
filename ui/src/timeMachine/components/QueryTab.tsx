// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineQueryTabName from 'src/timeMachine/components/QueryTabName'
import RightClick from 'src/clockface/components/right_click_menu/RightClick'
import TimeMachineQueriesTimer from 'src/timeMachine/components/QueriesTimer'

// Actions
import {
  setActiveQueryIndex,
  removeQuery,
  updateActiveQueryName,
  toggleQuery,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/QueryTab.scss'

// Types
import {AppState} from 'src/types/v2'
import {DashboardDraftQuery} from 'src/types/v2/dashboards'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  activeQueryIndex: number
  queryCount: number
}

interface DispatchProps {
  onSetActiveQueryIndex: typeof setActiveQueryIndex
  onRemoveQuery: typeof removeQuery
  onUpdateActiveQueryName: typeof updateActiveQueryName
  onToggleQuery: typeof toggleQuery
}

interface OwnProps {
  queryIndex: number
  query: DashboardDraftQuery
  queriesState: QueriesState
}

type Props = StateProps & DispatchProps & OwnProps

interface State {
  isEditingName: boolean
}
class TimeMachineQueryTab extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(props: Props): Partial<State> {
    if (props.queryIndex !== props.activeQueryIndex) {
      return {isEditingName: false}
    }

    return null
  }

  public state: State = {isEditingName: false}

  public render() {
    const {queryIndex, activeQueryIndex, query} = this.props
    const isActive = queryIndex === activeQueryIndex
    const activeClass = queryIndex === activeQueryIndex ? 'active' : ''

    return (
      <RightClick>
        <RightClick.Trigger>
          <div
            className={`query-tab ${activeClass}`}
            onClick={this.handleSetActive}
          >
            {this.showHideButton}
            <TimeMachineQueryTabName
              isActive={isActive}
              name={query.name}
              queryIndex={queryIndex}
              isEditing={this.state.isEditingName}
              onUpdate={this.handleUpdateName}
              onEdit={this.handleEditName}
              onCancelEdit={this.handleCancelEditName}
            />
            {this.queriesTimer}
            {this.removeButton}
          </div>
        </RightClick.Trigger>
        <RightClick.MenuContainer>
          <RightClick.Menu>
            <RightClick.MenuItem
              onClick={this.handleEditActiveQueryName}
              testID={'right-click--edit-tab'}
            >
              Edit
            </RightClick.MenuItem>
            <RightClick.MenuItem
              onClick={this.handleRemove}
              disabled={!this.isRemovable}
              testID={'right-click--remove-tab'}
            >
              Remove
            </RightClick.MenuItem>
          </RightClick.Menu>
        </RightClick.MenuContainer>
      </RightClick>
    )
  }

  private handleEditActiveQueryName = () => {
    this.handleSetActive()
    this.handleEditName()
  }

  private handleUpdateName = (queryName: string) => {
    this.props.onUpdateActiveQueryName(queryName)
  }

  private handleSetActive = (): void => {
    const {queryIndex, activeQueryIndex, onSetActiveQueryIndex} = this.props

    if (queryIndex === activeQueryIndex) {
      return
    }

    onSetActiveQueryIndex(queryIndex)
  }

  private handleCancelEditName = () => {
    this.setState({isEditingName: false})
  }

  private handleEditName = (): void => {
    this.setState({isEditingName: true})
  }

  private get queriesTimer(): JSX.Element {
    const {queriesState, queryIndex, activeQueryIndex} = this.props

    if (queryIndex === activeQueryIndex) {
      return (
        <TimeMachineQueriesTimer
          status={queriesState.loading}
          duration={queriesState.duration}
        />
      )
    }
  }

  private get removeButton(): JSX.Element {
    if (this.state.isEditingName || !this.isRemovable) {
      return null
    }

    return (
      <div className="query-tab--close" onClick={this.handleRemove}>
        <span className="icon remove" />
      </div>
    )
  }

  private get showHideButton(): JSX.Element {
    const {query} = this.props
    if (this.state.isEditingName || !this.isRemovable) {
      return null
    }

    const icon = query.hidden ? 'eye-open' : 'eye-closed'

    return (
      <div className="query-tab--hide" onClick={this.handleToggleView}>
        <span className={`icon ${icon}`} />
      </div>
    )
  }

  private get isRemovable(): boolean {
    return this.props.queryCount > 1
  }

  private handleRemove = (e: MouseEvent): void => {
    const {queryIndex, onRemoveQuery} = this.props

    e.stopPropagation()
    onRemoveQuery(queryIndex)
  }

  private handleToggleView = (e: MouseEvent): void => {
    const {queryIndex, onToggleQuery} = this.props

    e.stopPropagation()
    onToggleQuery(queryIndex)
  }
}

const mstp = (state: AppState) => {
  const {activeQueryIndex, draftQueries} = getActiveTimeMachine(state)

  return {activeQueryIndex, queryCount: draftQueries.length}
}

const mdtp = {
  onSetActiveQueryIndex: setActiveQueryIndex,
  onRemoveQuery: removeQuery,
  onUpdateActiveQueryName: updateActiveQueryName,
  onToggleQuery: toggleQuery,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueryTab)
