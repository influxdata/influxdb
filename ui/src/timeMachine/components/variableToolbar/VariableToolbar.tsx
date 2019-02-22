// Libraries
import React, {PureComponent} from 'react'

// Components
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Styles
import 'src/timeMachine/components/variableToolbar/VariableToolbar.scss'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import FetchVariables from 'src/timeMachine/components/variableToolbar/FetchVariables'

interface State {
  searchTerm: string
}

class VariableToolbar extends PureComponent<{}, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    return (
      <FetchVariables>
        {(variables, loading) => {
          return (
            <SpinnerContainer
              loading={loading}
              spinnerComponent={<TechnoSpinner />}
            >
              <div className="variable-toolbar">
                <SearchBar
                  onSearch={this.handleSearch}
                  resourceName={'Variables'}
                />
                <FancyScrollbar>
                  <div className="variables-toolbar--list">
                    {variables
                      .filter(v => {
                        return v.name.includes(this.state.searchTerm)
                      })
                      .map(v => {
                        return <VariableItem variable={v} key={v.id} />
                      })}
                  </div>
                </FancyScrollbar>
              </div>
            </SpinnerContainer>
          )
        }}
      </FetchVariables>
    )
  }

  private handleSearch = (searchTerm: string): void => {
    this.setState({searchTerm})
  }
}

export default VariableToolbar
