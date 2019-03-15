// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TransformToolbarFunctions from 'src/timeMachine/components/fluxFunctionsToolbar/TransformToolbarFunctions'
import FunctionCategory from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionCategory'
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {FLUX_FUNCTIONS} from 'src/shared/constants/fluxFunctions'

// Styles
import 'src/timeMachine/components/fluxFunctionsToolbar/FluxFunctionsToolbar.scss'

// Types
import {AppState} from 'src/types/v2'

interface OwnProps {
  onInsertFluxFunction: (functionName: string, text: string) => void
}

interface StateProps {
  activeQueryText: string
}

interface DispatchProps {
  onSetActiveQueryText: (script: string) => void
}

type Props = OwnProps & StateProps & DispatchProps

interface State {
  searchTerm: string
}

class FluxFunctionsToolbar extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  public render() {
    const {searchTerm} = this.state

    return (
      <div className="flux-functions-toolbar">
        <SearchBar onSearch={this.handleSearch} resourceName="Functions" />
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
                    onClickFunction={this.handleClickFunction}
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

  private handleClickFunction = (funcName: string, funcExample: string) => {
    this.props.onInsertFluxFunction(funcName, funcExample)
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
