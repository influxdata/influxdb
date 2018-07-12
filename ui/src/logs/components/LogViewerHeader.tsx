import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source, Namespace} from 'src/types'

import RadioButtons from 'src/reusable_ui/components/radio_buttons/RadioButtons'
import {ButtonShape, ComponentColor} from 'src/reusable_ui/types'
import Dropdown from 'src/shared/components/Dropdown'
import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import PageHeaderTitle from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import TimeRangeDropdown from 'src/logs/components/TimeRangeDropdown'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import {TimeRange} from 'src/types'
import {LiveUpdating} from 'src/types/logs'

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
  liveUpdating: LiveUpdating
  onChooseSource: (sourceID: string) => void
  onChooseNamespace: (namespace: Namespace) => void
  onChooseTimerange: (timeRange: TimeRange) => void
  onChangeLiveUpdatingStatus: () => void
  onShowOptionsOverlay: () => void
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
    const {timeRange, onShowOptionsOverlay} = this.props

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
        <TimeRangeDropdown
          onChooseTimeRange={this.handleChooseTimeRange}
          selected={timeRange}
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
    const buttons = ['icon play', 'icon pause']

    return (
      <RadioButtons
        customClass="logs-viewer--mode-toggle"
        shape={ButtonShape.Square}
        color={ComponentColor.Primary}
        buttons={buttons}
        onChange={onChangeLiveUpdatingStatus}
        activeButton={liveUpdating}
      />
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
