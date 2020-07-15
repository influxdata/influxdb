// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import TransformToolbarFunctions from 'src/timeMachine/components/fluxFunctionsToolbar/TransformToolbarFunctions'
import FunctionCategory from 'src/timeMachine/components/fluxFunctionsToolbar/FunctionCategory'
import FluxToolbarSearch from 'src/timeMachine/components/FluxToolbarSearch'
import {DapperScrollbars} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {setActiveQueryText} from 'src/timeMachine/actions'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {FLUX_FUNCTIONS} from 'src/shared/constants/fluxFunctions'

// Types
import {AppState, FluxToolbarFunction} from 'src/types'

interface OwnProps {
  onInsertFluxFunction: (func: FluxToolbarFunction) => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

interface State {
  searchTerm: string
}

class FluxFunctionsToolbar extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  public render() {
    const {searchTerm} = this.state

    return (
      <>
        <FluxToolbarSearch
          onSearch={this.handleSearch}
          resourceName="Functions"
        />
        <DapperScrollbars className="flux-toolbar--scroll-area">
          <div className="flux-toolbar--list">
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
        </DapperScrollbars>
      </>
    )
  }

  private handleSearch = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private handleClickFunction = (func: FluxToolbarFunction) => {
    this.props.onInsertFluxFunction(func)
  }
}

const mstp = (state: AppState) => {
  const activeQueryText = getActiveQuery(state).text

  return {activeQueryText}
}

const mdtp = {
  onSetActiveQueryText: setActiveQueryText,
}

const connector = connect(mstp, mdtp)

export default connector(ErrorHandling(FluxFunctionsToolbar))
