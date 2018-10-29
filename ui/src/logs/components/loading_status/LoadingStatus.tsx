import React, {PureComponent} from 'react'

import {
  EmptyState,
  ComponentSpacer,
  Alignment,
  ComponentSize,
} from 'src/clockface'

import {SearchStatus} from 'src/types/logs'
import {formatTime} from 'src/logs/utils'

interface Props {
  status: SearchStatus
  lower: number
  upper: number
}

class LoadingStatus extends PureComponent<Props> {
  public render() {
    return (
      <div className="logs-viewer--table-container">
        <EmptyState size={ComponentSize.Large}>
          {this.loadingSpinner}
          <h4>
            <ComponentSpacer align={Alignment.Center}>
              <>{this.loadingMessage}</>
            </ComponentSpacer>
            <ComponentSpacer align={Alignment.Center}>
              <>{this.description}</>
            </ComponentSpacer>
          </h4>
        </EmptyState>
      </div>
    )
  }

  private get description(): JSX.Element {
    switch (this.props.status) {
      case SearchStatus.SourceError:
        return (
          <>
            Try changing your <strong>Source</strong> or <strong>Bucket</strong>.
          </>
        )
      case SearchStatus.NoResults:
        return (
          <>
            Try changing the <strong>Time Range</strong> or{' '}
            <strong>Removing Filters</strong>
          </>
        )
      default:
        return <>{this.timeBounds}</>
    }
  }

  private get loadingSpinner(): JSX.Element {
    switch (this.props.status) {
      case SearchStatus.NoResults:
        return (
          <div className="logs-viewer--search-graphic">
            <div className="logs-viewer--graphic-empty" />
          </div>
        )
      case SearchStatus.UpdatingFilters:
      case SearchStatus.UpdatingTimeBounds:
      case SearchStatus.UpdatingSource:
      case SearchStatus.UpdatingBucket:
      case SearchStatus.Loading:
        return (
          <div className="logs-viewer--search-graphic">
            <div className="logs-viewer--graphic-log" />
            <div className="logs-viewer--graphic-magnifier-a">
              <div className="logs-viewer--graphic-magnifier-b" />
            </div>
          </div>
        )
      default:
        return null
    }
  }

  private get timeBounds(): JSX.Element {
    return (
      <div className="logs-viewer--searching-time">
        from <strong>{formatTime(this.props.upper)}</strong> to{' '}
        <strong>{formatTime(this.props.lower)}</strong>
      </div>
    )
  }

  private get loadingMessage(): string {
    switch (this.props.status) {
      case SearchStatus.UpdatingFilters:
        return 'Updating search filters...'
      case SearchStatus.NoResults:
        return 'No logs found'
      case SearchStatus.UpdatingTimeBounds:
        return 'Searching time bounds...'
      case SearchStatus.UpdatingSource:
        return 'Searching updated source...'
      case SearchStatus.UpdatingBucket:
        return 'Searching updated bucket...'
      case SearchStatus.SourceError:
        return 'Could not fetch logs for source.'
      case SearchStatus.Loading:
      default:
        return 'Searching...'
    }
  }
}

export default LoadingStatus
