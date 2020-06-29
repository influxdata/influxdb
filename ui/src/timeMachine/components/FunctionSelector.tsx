// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Input,
  FlexBox,
  FlexDirection,
  ComponentSize,
  TextBlock,
  InfluxColors,
} from '@influxdata/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import DurationInput from 'src/shared/components/DurationInput'

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
    const {isInCheckOverlay} = this.props

    const {searchTerm} = this.state
    return (
      <BuilderCard className="function-selector" testID="function-selector">
        <BuilderCard.Header title="Aggregate Functions" />
        <BuilderCard.Menu>
          <FlexBox
            direction={FlexDirection.Column}
            margin={ComponentSize.Small}
          >
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
            >
              <FlexBox.Child grow={2} testID="component-spacer--flex-child">
                <Input
                  className="tag-selector--search"
                  value={searchTerm}
                  onChange={this.handleSetSearchTerm}
                  placeholder="Search functions..."
                />
              </FlexBox.Child>
            </FlexBox>
            <FlexBox
              direction={FlexDirection.Row}
              margin={ComponentSize.Small}
              stretchToFitWidth
              testID="component-spacer"
            >
              <TextBlock
                textColor={InfluxColors.Sidewalk}
                text="Window period:"
              />
              <FlexBox.Child grow={2} testID="component-spacer--flex-child">
                <DurationInput
                  onSubmit={this.handleSelectAggregateWindow}
                  value={this.duration}
                  suggestions={this.durations}
                  submitInvalid={false}
                  validFunction={this.windowInputValid}
                />
              </FlexBox.Child>
            </FlexBox>
          </FlexBox>
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

  private get autoLabel(): string {
    const {autoWindowPeriod} = this.props
    return autoWindowPeriod
      ? `${AGG_WINDOW_AUTO} (${millisecondsToDuration(autoWindowPeriod)})`
      : AGG_WINDOW_AUTO
  }

  private get duration(): string {
    const {aggregateWindow} = this.props

    if (!aggregateWindow.period || aggregateWindow.period === AGG_WINDOW_AUTO) {
      return this.autoLabel
    }

    return aggregateWindow.period
  }

  private get durations(): string[] {
    return this.props.isInCheckOverlay
      ? DURATIONS
      : [this.autoLabel, AGG_WINDOW_NONE, ...DURATIONS]
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

  private handleSelectAggregateWindow = (input: string) => {
    if (input.startsWith(AGG_WINDOW_AUTO)) {
      this.props.onSelectAggregateWindow(AGG_WINDOW_AUTO)
      return
    }
    this.props.onSelectAggregateWindow(input)
  }

  private windowInputValid = (input: string): boolean =>
    input == 'none' || input == this.autoLabel
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

export default connect<StateProps, DispatchProps>(mstp, mdtp)(FunctionSelector)
