// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Input, ComponentSize} from 'src/clockface'

// Utils
import DefaultDebouncer, {Debouncer} from 'src/shared/utils/debouncer'
import {CancellationError} from 'src/utils/restartable'

const SEARCH_DEBOUNCE_MS = 350

interface Props {
  onSearch: (searchTerm: string) => Promise<void>
}

interface State {
  searchTerm: string
}

class BuilderCardSearchBar extends PureComponent<Props, State> {
  public state: State = {searchTerm: ''}

  private debouncer: Debouncer = new DefaultDebouncer()

  public render() {
    const {searchTerm} = this.state

    return (
      <div className="builder-card-search-bar">
        <Input
          size={ComponentSize.Small}
          placeholder="Search..."
          value={searchTerm}
          onChange={this.handleChange}
        />
      </div>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState({searchTerm: e.target.value}, () =>
      this.debouncer.call(this.emitChange, SEARCH_DEBOUNCE_MS)
    )
  }

  private emitChange = async () => {
    const {onSearch} = this.props
    const {searchTerm} = this.state

    try {
      await onSearch(searchTerm)
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      this.setState({searchTerm: ''})
    }
  }
}

export default BuilderCardSearchBar
