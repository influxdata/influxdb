// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from '@influxdata/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import DurationSelector, {
  DurationOption,
} from 'src/shared/components/DurationSelector'

// Actions
import {
  selectBuilderFunction,
  selectAggregateWindow,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getIsInCheckOverlay,
  getActiveWindowPeriod,
} from 'src/timeMachine/selectors'
import {millisecondsToDuration} from 'src/shared/utils/duration'

// Constants
import {
  FUNCTIONS,
  AGG_WINDOW_AUTO,
  AGG_WINDOW_NONE,
  DURATIONS,
} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState, BuilderConfig} from 'src/types'

const FUNCTION_NAMES = FUNCTIONS.map(f => f.name)

interface StateProps {
  autoWindowPeriod: number | null
  aggregateWindow: BuilderConfig['aggregateWindow']
  selectedFunctions: BuilderConfig['functions']
  isInCheckOverlay: boolean
}

interface DispatchProps {
  onSelectFunction: typeof selectBuilderFunction
  onSelectAggregateWindow: typeof selectAggregateWindow
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
}

class FunctionSelector extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  public render() {
    const {
      selectedFunctions,
      onSelectAggregateWindow,
      isInCheckOverlay,
    } = this.props

    const {searchTerm} = this.state

    return (
      <BuilderCard className="function-selector" testID="function-selector">
        <BuilderCard.Header title="Aggregate Functions" />
        <BuilderCard.Menu>
          <DurationSelector
            onSelectDuration={onSelectAggregateWindow}
            selectedDuration={this.duration}
            durations={this.durations}
            disabled={!selectedFunctions.length}
          />
          <Input
            className="tag-selector--search"
            value={searchTerm}
            onChange={this.handleSetSearchTerm}
            placeholder="Search functions..."
          />
        </BuilderCard.Menu>
        <SelectorList
          items={this.functions}
          selectedItems={this.selectedFunctions}
          onSelectItem={this.handleSelectFunction}
          multiSelect={!isInCheckOverlay}
        />
      </BuilderCard>
    )
  }

  private get duration(): string {
    const {aggregateWindow} = this.props

    return aggregateWindow.period || AGG_WINDOW_AUTO
  }

  private get durations(): DurationOption[] {
    return this.props.isInCheckOverlay
      ? DURATIONS
      : [...this.autoNoneDurations, ...DURATIONS]
  }

  private get autoNoneDurations(): DurationOption[] {
    const {autoWindowPeriod} = this.props
    const autoLabel = autoWindowPeriod
      ? `Auto (${millisecondsToDuration(autoWindowPeriod)})`
      : 'Auto'

    return [
      {duration: AGG_WINDOW_AUTO, displayText: autoLabel},
      {duration: AGG_WINDOW_NONE, displayText: 'None'},
    ]
  }

  private get functions(): string[] {
    return FUNCTION_NAMES.filter(f => f.includes(this.state.searchTerm))
  }

  private get selectedFunctions(): string[] {
    return this.props.selectedFunctions.map(f => f.name)
  }

  private handleSetSearchTerm = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({searchTerm: e.target.value})
  }

  private handleSelectFunction = (functionName: string) => {
    const {isInCheckOverlay, selectedFunctions, onSelectFunction} = this.props

    if (isInCheckOverlay && selectedFunctions[0].name === functionName) {
      // Disallow empty aggreegate selections in check overlay
      return
    }

    onSelectFunction(functionName)
  }
}

const mstp = (state: AppState): StateProps => {
  const {builderConfig} = getActiveQuery(state)
  const {functions: selectedFunctions, aggregateWindow} = builderConfig

  return {
    selectedFunctions,
    aggregateWindow,
    autoWindowPeriod: getActiveWindowPeriod(state),
    isInCheckOverlay: getIsInCheckOverlay(state),
  }
}

const mdtp = {
  onSelectFunction: selectBuilderFunction,
  onSelectAggregateWindow: selectAggregateWindow,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(FunctionSelector)
