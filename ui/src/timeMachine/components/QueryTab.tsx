// Libraries
import React, {PureComponent, MouseEvent, RefObject, createRef} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import classnames from 'classnames'

// Components
import TimeMachineQueryTabName from 'src/timeMachine/components/QueryTabName'
import TimeMachineQueriesTimer from 'src/timeMachine/components/QueriesTimer'
import {RightClick, ComponentColor, Icon, IconFont} from '@influxdata/clockface'

// Actions
import {
  setActiveQueryIndex,
  removeQuery,
  updateActiveQueryName,
  toggleQuery,
} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'
import {DashboardDraftQuery} from 'src/types/dashboards'

interface OwnProps {
  queryIndex: number
  query: DashboardDraftQuery
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

interface State {
  isEditingName: boolean
}

class TimeMachineQueryTab extends PureComponent<Props, State> {
  private triggerRef: RefObject<HTMLDivElement> = createRef()

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
    const queryTabClass = classnames('query-tab', {
      'query-tab__active': queryIndex === activeQueryIndex,
      'query-tab__hidden': query.hidden,
    })

    return (
      <>
        <div
          className={queryTabClass}
          onClick={this.handleSetActive}
          ref={this.triggerRef}
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
        <RightClick triggerRef={this.triggerRef} color={ComponentColor.Primary}>
          <RightClick.MenuItem
            onClick={this.handleEditActiveQueryName}
            testID="right-click--edit-tab"
          >
            Edit
          </RightClick.MenuItem>
          <RightClick.MenuItem
            onClick={this.handleRemove}
            disabled={!this.isRemovable}
            testID="right-click--remove-tab"
          >
            Remove
          </RightClick.MenuItem>
        </RightClick>
      </>
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
    const {queryIndex, activeQueryIndex} = this.props

    if (queryIndex === activeQueryIndex) {
      return <TimeMachineQueriesTimer />
    }
  }

  private get removeButton(): JSX.Element {
    if (this.state.isEditingName || !this.isRemovable) {
      return null
    }

    return (
      <div className="query-tab--close" onClick={this.handleRemove}>
        <Icon glyph={IconFont.Remove} />
      </div>
    )
  }

  private get showHideButton(): JSX.Element {
    const {query} = this.props
    if (this.state.isEditingName || !this.isRemovable) {
      return null
    }

    const icon = query.hidden ? IconFont.EyeClosed : IconFont.EyeOpen

    return (
      <div className="query-tab--hide" onClick={this.handleToggleView}>
        <Icon glyph={icon} />
      </div>
    )
  }

  private get isRemovable(): boolean {
    return this.props.queryCount > 1
  }

  private handleRemove = (): void => {
    const {queryIndex, onRemoveQuery} = this.props

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

const connector = connect(mstp, mdtp)
export default connector(TimeMachineQueryTab)
