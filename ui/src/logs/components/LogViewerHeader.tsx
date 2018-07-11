import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source, Namespace} from 'src/types'
import classnames from 'classnames'

import Dropdown from 'src/shared/components/Dropdown'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import PageHeaderTitle from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import TimeMarkerDropdown from 'src/logs/components/TimeMarkerDropdown'
import TimeWindowDropdown from 'src/logs/components/TimeWindowDropdown'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import {TimeRange, TimeWindow, TimeMarker} from 'src/types/logs'

interface SourceItem {
  id: string
  text: string
}

interface Props {
  currentNamespace: Namespace
  availableSources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  liveUpdating: boolean
  onChooseSource: (sourceID: string) => void
  onChooseNamespace: (namespace: Namespace) => void
  onChangeLiveUpdatingStatus: () => void
  onShowOptionsOverlay: () => void
  timeRange: TimeRange
  onSetTimeMarker: (timeMarker: TimeMarker) => void
  onSetTimeWindow: (timeWindow: TimeWindow) => void
}

class LogViewerHeader extends PureComponent<Props> {
  public render(): JSX.Element {
    return (
      <PageHeader
        titleComponents={this.renderHeaderTitle}
        fullWidth={true}
        optionsComponents={this.optionsComponents}
      />
    )
  }

  private get renderHeaderTitle(): JSX.Element {
    return (
      <>
        {this.status}
        <PageHeaderTitle title="Log Viewer" />
      </>
    )
  }

  private get optionsComponents(): JSX.Element {
    const {onShowOptionsOverlay, onSetTimeWindow, onSetTimeMarker} = this.props

    // Todo: Replace w/ getDeep
    const timeRange = _.get(this.props, 'timeRange', {
      upper: null,
      lower: 'now() - 1m',
      seconds: 60,
      windowOption: '1m',
      timeOption: 'now',
    })

    return (
      <>
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
        <TimeWindowDropdown
          selectedTimeWindow={timeRange.windowOption}
          onSetTimeWindow={onSetTimeWindow}
        />
        <TimeMarkerDropdown
          onSetTimeMarker={onSetTimeMarker}
          selectedTimeMarker={timeRange.timeOption}
        />
        <Authorized requiredRole={EDITOR_ROLE}>
          <button
            className="btn btn-sm btn-square btn-default"
            onClick={onShowOptionsOverlay}
          >
            <span className="icon cog-thick" />
          </button>
        </Authorized>
      </>
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
