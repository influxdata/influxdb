import React, {PureComponent} from 'react'

interface Props {
  thing: string
}

class LogsSearchBar extends PureComponent<Props> {
  public render() {
    return (
      <div className="logs-viewer--search-container">
        <div className="logs-viewer--search">
          <div className="logs-viewer--search-input">
            <input
              type="text"
              placeholder="Search logs using Keywords or Regular Expressions..."
              defaultValue=""
              className="form-control input-sm"
              spellCheck={false}
              autoComplete="off"
            />
            <span className="icon search" />
          </div>
          <button className="btn btn-sm btn-primary">
            <span className="icon search" />
            Search
          </button>
        </div>
        <div className="logs-viewer--filters-container">
          <label className="logs-viewer--results-text">
            Query returned <strong>2,401 Events</strong>
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
}

export default LogsSearchBar
