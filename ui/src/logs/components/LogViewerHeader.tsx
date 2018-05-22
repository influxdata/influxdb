import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source, Namespace} from 'src/types'
import Dropdown from 'src/shared/components/Dropdown'
import TimeRangeDropdown from 'src/logs/components/TimeRangeDropdown'

import {TimeRange} from 'src/types'

interface SourceItem {
  id: string
  text: string
}

interface Props {
  availableSources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  timeRange: TimeRange
  onChooseSource: (sourceID: string) => void
  onChooseNamespace: (namespace: Namespace) => void
  onChooseTimerange: (timeRange: TimeRange) => void
}

class LogViewerHeader extends PureComponent<Props> {
  public render(): JSX.Element {
    const {timeRange} = this.props
    return (
      <>
        <Dropdown
          className="dropdown-300"
          items={this.sourceDropDownItems}
          selected={this.selectedSource}
          onChoose={this.handleChooseSource}
        />
        <Dropdown
          className="dropdown-300"
          items={this.namespaceDropDownItems}
          selected={this.selectedNamespace}
          onChoose={this.handleChooseNamespace}
        />
        <TimeRangeDropdown
          onChooseTimeRange={this.handleChooseTimeRange}
          selected={timeRange}
        />
      </>
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

    return this.sourceDropDownItems[0].text
  }

  private get selectedNamespace(): string {
    if (_.isEmpty(this.namespaceDropDownItems)) {
      return ''
    }

    return this.namespaceDropDownItems[0].text
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
