import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

interface Props {
  searchString: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
  onSearch: () => void
}

class LogsSearchBar extends PureComponent<Props> {
  public render() {
    const {searchString, onSearch, onChange} = this.props

    return (
      <div className="logs-viewer--search-bar">
        <div className="logs-viewer--search-input">
          <input
            type="text"
            placeholder="Search logs using Keywords or Regular Expressions..."
            value={searchString}
            onChange={onChange}
            onKeyDown={this.handleInputKeyDown}
            className="form-control input-sm"
            spellCheck={false}
            autoComplete="off"
          />
          <span className="icon search" />
        </div>
        <button className="btn btn-sm btn-primary" onClick={onSearch}>
          <span className="icon search" />
          Search
        </button>
      </div>
    )
  }

  private handleInputKeyDown = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter') {
      return this.props.onSearch()
    }
  }
}

export default LogsSearchBar
