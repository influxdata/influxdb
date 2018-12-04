// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Input, ComponentSize, ComponentStatus} from 'src/clockface'

// Utils
import DefaultDebouncer, {Debouncer} from 'src/shared/utils/debouncer'
import {CancellationError} from 'src/utils/restartable'

const SEARCH_DEBOUNCE_MS = 500

interface Props {
  onSearch: (searchTerm: string) => Promise<void>
}

interface State {
  searchTerm: string
  status: ComponentStatus
}

class SelectorCardSearchBar extends PureComponent<Props, State> {
  public state: State = {searchTerm: '', status: ComponentStatus.Default}

  private debouncer: Debouncer = new DefaultDebouncer()

  public render() {
    const {searchTerm, status} = this.state

    return (
      <div className="selector-card-search-bar">
        <Input
          size={ComponentSize.Small}
          placeholder="Search..."
          value={searchTerm}
          status={status}
          onChange={this.handleChange}
        />
      </div>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    this.setState(
      {searchTerm: e.target.value, status: ComponentStatus.Loading},
      () => this.debouncer.call(this.emitChange, SEARCH_DEBOUNCE_MS)
    )
  }

  private emitChange = async () => {
    const {onSearch} = this.props
    const {searchTerm} = this.state

    try {
      await onSearch(searchTerm)

      this.setState({status: ComponentStatus.Default})
    } catch (e) {
      if (e instanceof CancellationError) {
        return
      }

      this.setState({
        searchTerm: '',
        status: ComponentStatus.Default,
      })
    }
  }
}

export default SelectorCardSearchBar
