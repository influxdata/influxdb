import React, {PureComponent, ChangeEvent, KeyboardEvent} from 'react'

interface Props {
  numResults: number
  searchString: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
  onSearch: () => void
}

class LogsSearchBar extends PureComponent<Props> {
  public render() {
    const {searchString, onSearch, onChange, numResults} = this.props

    return (
      <div className="logs-viewer--search-container">
        <div className="logs-viewer--search">
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
        <div className="logs-viewer--filters-container">
          <label className="logs-viewer--results-text">
            Query returned <strong>{numResults} Events</strong>
          </label>
          <ul className="logs-viewer--filters">
            <li className="logs-viewer--filter">
              <span>host='swoggle'</span>
              <button className="logs-viewer--filter-remove" />
            </li>
            <li className="logs-viewer--filter">
              <span>appname='plunger'</span>
              <button className="logs-viewer--filter-remove" />
            </li>
            <li className="logs-viewer--filter">
              <span>appname='bug-zapper'</span>
              <button className="logs-viewer--filter-remove" />
            </li>
          </ul>
        </div>
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
