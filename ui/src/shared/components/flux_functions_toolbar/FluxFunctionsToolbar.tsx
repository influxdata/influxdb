// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TransformToolbarFunctions from 'src/shared/components/flux_functions_toolbar/TransformToolbarFunctions'
import FunctionCategory from 'src/shared/components/flux_functions_toolbar/FunctionCategory'
import SearchBar from 'src/shared/components/flux_functions_toolbar/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {setActiveQueryText} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveQuery} from 'src/shared/selectors/timeMachines'

// Constants
import {FLUX_FUNCTIONS} from 'src/shared/constants/fluxFunctions'

// Types
import {AppState} from 'src/types/v2'

interface StateProps {
  activeQueryText: string
}

interface DispatchProps {
  onSetActiveQueryText: (script: string) => void
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
}

class FluxFunctionsToolbar extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  public render() {
    const {searchTerm} = this.state

    return (
      <div className="flux-functions-toolbar">
        <SearchBar onSearch={this.handleSearch} />
        <FancyScrollbar>
          <div className="flux-functions-toolbar--list">
            <TransformToolbarFunctions
              funcs={FLUX_FUNCTIONS}
              searchTerm={searchTerm}
            >
              {sortedFunctions =>
                Object.entries(sortedFunctions).map(([category, funcs]) => (
                  <FunctionCategory
                    key={category}
                    category={category}
                    funcs={funcs}
                    onClickFunction={this.handleUpdateScript}
                  />
                ))
              }
            </TransformToolbarFunctions>
          </div>
        </FancyScrollbar>
      </div>
    )
  }

  private handleSearch = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private handleUpdateScript = (funcName: string, funcExample: string) => {
    const {activeQueryText, onSetActiveQueryText} = this.props

    if (funcName === 'from') {
      onSetActiveQueryText(`${activeQueryText}\n${funcExample}`)
    } else {
      onSetActiveQueryText(`${activeQueryText}\n  |> ${funcExample}`)
    }
  }
}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text

  return {activeQueryText}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(ErrorHandling(FluxFunctionsToolbar))
