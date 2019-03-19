// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from 'src/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'

// Actions
import {selectFunction} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState, BuilderConfig} from 'src/types/v2'

const FUNCTION_NAMES = FUNCTIONS.map(f => f.name)

interface StateProps {
  selectedFunctions: BuilderConfig['functions']
}

interface DispatchProps {
  onSelectFunction: (fnName: string) => void
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
}

class FunctionSelector extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  public render() {
    const {onSelectFunction} = this.props
    const {searchTerm} = this.state

    return (
      <div className="function-selector">
        <h3>Aggregate Functions</h3>
        <Input
          customClass={'function-selector--search'}
          value={searchTerm}
          onChange={this.handleSetSearchTerm}
          placeholder="Search functions..."
        />
        <SelectorList
          items={this.functions}
          selectedItems={this.selectedFunctions}
          onSelectItem={onSelectFunction}
        />
      </div>
    )
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
  const selectedFunctions = getActiveQuery(state).builderConfig.functions

  return {selectedFunctions}
}

const mdtp = {
  onSelectFunction: selectFunction,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(FunctionSelector)
