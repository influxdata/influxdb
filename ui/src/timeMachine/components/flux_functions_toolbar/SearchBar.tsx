// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {debounce} from 'lodash'

// Components
import {Input, IconFont} from 'src/clockface'

// Types
import {InputType} from 'src/clockface/components/inputs/Input'

interface Props {
  onSearch: (s: string) => void
}

interface State {
  searchTerm: string
}

const DEBOUNCE_MS = 100

class SearchBar extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
    }

    this.handleSearch = debounce(this.handleSearch, DEBOUNCE_MS)
  }

  public render() {
    return (
      <div className="flux-functions-toolbar--search">
        <Input
          type={InputType.Text}
          icon={IconFont.Search}
          placeholder="Filter Functions..."
          onChange={this.handleChange}
          value={this.state.searchTerm}
        />
      </div>
    )
  }

  private handleSearch = (): void => {
    this.props.onSearch(this.state.searchTerm)
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value}, this.handleSearch)
  }
}

export default SearchBar
