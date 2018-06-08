import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

interface Props {
  searchString: string
  onSearch: (value: string) => void
}

interface State {
  searchTerm: string
}

class LogsSearchBar extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: props.searchString,
    }
  }

  public render() {
    const {searchTerm} = this.state

    return (
      <div className="logs-viewer--search-bar">
        <div className="logs-viewer--search-input">
          <input
            type="text"
            placeholder="Search logs using keywords or regular expressions..."
            value={searchTerm}
            onChange={this.handleChange}
            onKeyDown={this.handleInputKeyDown}
            className="form-control input-sm"
            spellCheck={false}
            autoComplete="off"
          />
          <span className="icon search" />
        </div>
        <button className="btn btn-sm btn-default" onClick={this.handleSearch}>
          Search
        </button>
      </div>
    )
  }

  private handleSearch = () => {
    this.props.onSearch(this.state.searchTerm)
  }

  private handleInputKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      return this.handleSearch()
    }
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

export default LogsSearchBar
