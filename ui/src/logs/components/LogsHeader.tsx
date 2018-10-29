import _ from 'lodash'
import React, {PureComponent} from 'react'
import {Source, Bucket} from 'src/types/v2'

import {
  Dropdown,
  IconFont,
  DropdownMode,
  Button,
  ButtonShape,
} from 'src/clockface'
import {Page} from 'src/pageLayout'
// import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import LiveUpdatingStatus from 'src/logs/components/LiveUpdatingStatus'

interface SourceItem {
  id: string
  text: string
}

interface Props {
  currentBucket: Bucket
  availableSources: Source[]
  currentSource: Source | null
  currentBuckets: Bucket[]
  onChooseSource: (sourceID: string) => void
  onChooseBucket: (bucket: Bucket) => void
  liveUpdating: boolean
  onChangeLiveUpdatingStatus: () => void
  onShowOptionsOverlay: () => void
}

const EMPTY_ID = 'none'

class LogsHeader extends PureComponent<Props> {
  public render(): JSX.Element {
    const {
      liveUpdating,
      onChangeLiveUpdatingStatus,
      onShowOptionsOverlay,
    } = this.props

    return (
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <LiveUpdatingStatus
            onChangeLiveUpdatingStatus={onChangeLiveUpdatingStatus}
            liveUpdating={liveUpdating}
          />
          <Page.Title title="Log Viewer" />
        </Page.Header.Left>
        <Page.Header.Right>
          <Dropdown
            widthPixels={200}
            selectedID={this.selectedSource}
            onChange={this.handleChooseSource}
            titleText="Sources"
          >
            {this.sourceDropDownItems}
          </Dropdown>
          <Dropdown
            widthPixels={160}
            icon={IconFont.Disks}
            selectedID={this.selectedBucket}
            onChange={this.handleChooseBucket}
            titleText="Buckets"
            mode={DropdownMode.ActionList}
          >
            {this.bucketDropDownItems}
          </Dropdown>
          {/* <Authorized requiredRole={EDITOR_ROLE}> */}
          <Button
            onClick={onShowOptionsOverlay}
            shape={ButtonShape.Square}
            icon={IconFont.CogThick}
          />
          {/* </Authorized> */}
        </Page.Header.Right>
      </Page.Header>
    )
  }

  private handleChooseSource = (item: SourceItem) => {
    this.props.onChooseSource(item.id)
  }

  private handleChooseBucket = (bucket: Bucket) => {
    this.props.onChooseBucket(bucket)
  }

  private get selectedSource(): string {
    const {availableSources} = this.props
    if (_.isEmpty(availableSources)) {
      return EMPTY_ID
    }

    const id = _.get(this.props, 'currentSource.id', null)

    if (id === null) {
      return availableSources[0].id
    }

    const currentItem = _.find(availableSources, s => s.id === id)

    if (currentItem) {
      return currentItem.id
    }

    return EMPTY_ID
  }

  private get selectedBucket(): string {
    const {currentBucket} = this.props

    if (!currentBucket) {
      return EMPTY_ID
    }

    return `${currentBucket.name}.${currentBucket.rp}`
  }

  private get bucketDropDownItems() {
    const {currentBuckets} = this.props

    if (_.isEmpty(currentBuckets)) {
      return this.renderEmptyDropdown('No Buckets Found')
    }

    return currentBuckets.map((bucket: Bucket) => {
      const bucketText = `${bucket.name}.${bucket.rp}`
      return (
        <Dropdown.Item value={bucket} key={bucketText} id={bucketText}>
          {bucketText}
        </Dropdown.Item>
      )
    })
  }

  private get sourceDropDownItems(): JSX.Element[] {
    const {availableSources} = this.props

    if (_.isEmpty(availableSources)) {
      return this.renderEmptyDropdown('No Sources Found')
    }

    return availableSources.map(source => {
      const sourceText = `${source.name} @ ${source.url}`
      return (
        <Dropdown.Item value={source} id={source.id} key={source.id}>
          {sourceText}
        </Dropdown.Item>
      )
    })
  }

  private renderEmptyDropdown(text: string): JSX.Element[] {
    return [<Dropdown.Divider key={EMPTY_ID} id={EMPTY_ID} text={text} />]
  }
}

export default LogsHeader
