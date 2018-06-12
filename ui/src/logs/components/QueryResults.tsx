import React, {PureComponent} from 'react'

interface Props {
  count: number
  queryCount: number
}

class QueryResults extends PureComponent<Props> {
  public render() {
    const {count} = this.props

    if (this.isPending) {
      return (
        <>
          <div className="logs-viewer--results-spinner" />
          <label className="logs-viewer--results-text">Querying...</label>
        </>
      )
    }

    return (
      <label className="logs-viewer--results-text">
        Query returned <strong>{count} Events</strong>
      </label>
    )
  }

  private get isPending(): boolean {
    const {queryCount} = this.props
    return queryCount > 0
  }
}

export default QueryResults
