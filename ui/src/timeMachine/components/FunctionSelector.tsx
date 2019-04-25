// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from '@influxdata/clockface'
import SelectorList from 'src/timeMachine/components/SelectorList'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

// Actions
import {selectFunction} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState, BuilderConfig} from 'src/types'

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
      <BuilderCard className="function-selector">
        <BuilderCard.Header title="Aggregate Functions" />
        <BuilderCard.Menu>
          <Input
            className="function-selector--search"
            value={searchTerm}
            onChange={this.handleSetSearchTerm}
            placeholder="Search functions..."
          />
        </BuilderCard.Menu>
        <SelectorList
          items={this.functions}
          selectedItems={this.selectedFunctions}
          onSelectItem={onSelectFunction}
        />
      </BuilderCard>
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
