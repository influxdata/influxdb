// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineQueryTabName from 'src/shared/components/TimeMachineQueryTabName'
import RightClick from 'src/clockface/components/right_click_menu/RightClick'

// Actions
import {
  setActiveQueryIndex,
  removeQuery,
  updateActiveQueryName,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueryTab.scss'

// Types
import {AppState, DashboardQuery} from 'src/types/v2'

interface StateProps {
  activeQueryIndex: number
  queryCount: number
}

interface DispatchProps {
  onSetActiveQueryIndex: typeof setActiveQueryIndex
  onRemoveQuery: typeof removeQuery
  onUpdateActiveQueryName: typeof updateActiveQueryName
}

interface OwnProps {
  queryIndex: number
  query: DashboardQuery
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
            className={`time-machine-query-tab ${activeClass}`}
            onClick={this.handleSetActive}
          >
            <TimeMachineQueryTabName
              isActive={isActive}
              name={query.name}
              queryIndex={queryIndex}
              isEditing={this.state.isEditingName}
              onUpdate={this.handleUpdateName}
              onEdit={this.handleEditName}
              onCancelEdit={this.handleCancelEditName}
            />
            {this.removeButton}
          </div>
        </RightClick.Trigger>
        <RightClick.MenuContainer>
          <RightClick.Menu>
            <RightClick.MenuItem onClick={this.handleEditActiveQueryName}>
              Edit
            </RightClick.MenuItem>
            <RightClick.MenuItem
              onClick={this.handleRemove}
              disabled={!this.isRemovable}
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

  private get removeButton(): JSX.Element {
    if (this.state.isEditingName || !this.isRemovable) {
      return null
    }

    return (
      <div
        className="time-machine-query-tab--close"
        onClick={this.handleRemove}
      >
        <span className="icon remove" />
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
}

const mstp = (state: AppState) => {
  const {activeQueryIndex, draftQueries} = getActiveTimeMachine(state)

  return {activeQueryIndex, queryCount: draftQueries.length}
}

const mdtp = {
  onSetActiveQueryIndex: setActiveQueryIndex,
  onRemoveQuery: removeQuery,
  onUpdateActiveQueryName: updateActiveQueryName,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueryTab)
