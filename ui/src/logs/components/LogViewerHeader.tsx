import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source, Namespace} from 'src/types'
import classnames from 'classnames'
import Dropdown from 'src/shared/components/Dropdown'
import TimeRangeDropdown from 'src/logs/components/TimeRangeDropdown'

import {TimeRange} from 'src/types'

interface SourceItem {
  id: string
  text: string
}

interface Props {
  currentNamespace: Namespace
  availableSources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  timeRange: TimeRange
  liveUpdating: boolean
  onChooseSource: (sourceID: string) => void
  onChooseNamespace: (namespace: Namespace) => void
  onChooseTimerange: (timeRange: TimeRange) => void
  onChangeLiveUpdatingStatus: () => void
}

class LogViewerHeader extends PureComponent<Props> {
  public render(): JSX.Element {
    const {timeRange} = this.props
    return (
      <div className="page-header full-width">
        <div className="page-header--container">
          <div className="page-header--left">
            {this.status}
            <h1 className="page-header--title logs-viewer-header-title">
              Log Viewer
            </h1>
          </div>
          <div className="page-header--right">
            <Dropdown
              className="dropdown-300"
              items={this.sourceDropDownItems}
              selected={this.selectedSource}
              onChoose={this.handleChooseSource}
            />
            <Dropdown
              className="dropdown-180"
              iconName="disks"
              items={this.namespaceDropDownItems}
              selected={this.selectedNamespace}
              onChoose={this.handleChooseNamespace}
            />
            <TimeRangeDropdown
              onChooseTimeRange={this.handleChooseTimeRange}
              selected={timeRange}
            />
          </div>
        </div>
      </div>
    )
  }

  private get status(): JSX.Element {
    const {liveUpdating, onChangeLiveUpdatingStatus} = this.props

    return (
      <ul className="nav nav-tablist nav-tablist-sm logs-viewer--mode-toggle">
        <li
          className={classnames({active: liveUpdating})}
          onClick={onChangeLiveUpdatingStatus}
        >
          <span className="icon play" />
        </li>
        <li
          className={classnames({active: !liveUpdating})}
          onClick={onChangeLiveUpdatingStatus}
        >
          <span className="icon pause" />
        </li>
      </ul>
    )
  }

  private handleChooseTimeRange = (timerange: TimeRange) => {
    this.props.onChooseTimerange(timerange)
  }

  private handleChooseSource = (item: SourceItem) => {
    this.props.onChooseSource(item.id)
  }

  private handleChooseNamespace = (namespace: Namespace) => {
    this.props.onChooseNamespace(namespace)
  }

  private get selectedSource(): string {
    if (_.isEmpty(this.sourceDropDownItems)) {
      return ''
    }

    const id = _.get(this.props, 'currentSource.id', '')
    const currentItem = _.find(this.sourceDropDownItems, item => {
      return item.id === id
    })

    if (currentItem) {
      return currentItem.text
    }

    return ''
  }

  private get selectedNamespace(): string {
    const {currentNamespace} = this.props

    if (!currentNamespace) {
      return ''
    }

    return `${currentNamespace.database}.${currentNamespace.retentionPolicy}`
  }

  private get namespaceDropDownItems() {
    const {currentNamespaces} = this.props

    return currentNamespaces.map(namespace => {
      return {
        text: `${namespace.database}.${namespace.retentionPolicy}`,
        ...namespace,
      }
    })
  }

  private get sourceDropDownItems(): SourceItem[] {
    const {availableSources} = this.props

    return availableSources.map(source => {
      return {
        text: `${source.name} @ ${source.url}`,
        id: source.id,
      }
    })
  }
}

export default LogViewerHeader
