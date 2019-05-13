// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from '@influxdata/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import WindowSelector from 'src/timeMachine/components/WindowSelector'

// Actions
import {
  selectFunction,
  selectAggregateWindow,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {
  FUNCTIONS,
  AGG_WINDOW_AUTO,
} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState, BuilderConfig} from 'src/types'

const FUNCTION_NAMES = FUNCTIONS.map(f => f.name)

interface StateProps {
  aggregateWindow: BuilderConfig['aggregateWindow']
  selectedFunctions: BuilderConfig['functions']
}

interface DispatchProps {
  onSelectFunction: typeof selectFunction
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
      onSelectFunction,
      selectedFunctions,
      onSelectAggregateWindow,
    } = this.props

    const {searchTerm} = this.state

    return (
      <BuilderCard className="function-selector" testID="function-selector">
        <BuilderCard.Header title="Aggregate Functions" />
        <BuilderCard.Menu>
          <WindowSelector
            onSelect={onSelectAggregateWindow}
            period={this.period}
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
          onSelectItem={onSelectFunction}
          multiSelect={true}
        />
      </BuilderCard>
    )
  }

  private get period(): string {
    const {aggregateWindow} = this.props
    return aggregateWindow.period || AGG_WINDOW_AUTO
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
}

const mstp = (state: AppState) => {
  const {functions: selectedFunctions, aggregateWindow} = getActiveQuery(
    state
  ).builderConfig

  return {selectedFunctions, aggregateWindow}
}

const mdtp = {
  onSelectFunction: selectFunction,
  onSelectAggregateWindow: selectAggregateWindow,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(FunctionSelector)
