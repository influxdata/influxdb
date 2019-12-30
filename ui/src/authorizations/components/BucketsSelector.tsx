// Libraries
import React, {PureComponent} from 'react'

// Components
import SelectorList from 'src/shared/components/selectorList/SelectorList'
import BucketsTabBody from 'src/authorizations/components/BucketsTabBody'
import {BucketTab} from 'src/authorizations/utils/permissions'
import BucketsTabSelector from 'src/authorizations/components/BucketsTabSelector'

// Types
import {Bucket} from 'src/types'
import {
  FlexBox,
  AlignItems,
  FlexDirection,
  Button,
  EmptyState,
  ComponentSize,
} from '@influxdata/clockface'

interface Props {
  buckets: Bucket[]
  onSelect: (id: string) => void
  onSelectAll: () => void
  onDeselectAll: () => void
  selectedBuckets: string[]
  title: string
  activeTab: BucketTab
  onTabClick: (tab: BucketTab) => void
}

class BucketsSelector extends PureComponent<Props> {
  render() {
    const {title, activeTab, onTabClick} = this.props

    return (
      <FlexBox
        alignItems={AlignItems.Stretch}
        direction={FlexDirection.Column}
        margin={ComponentSize.Medium}
      >
        <div className="title">{title}</div>
        <BucketsTabSelector
          tabs={this.bucketTabs}
          activeTab={activeTab}
          onClick={onTabClick}
        />
        {this.builderCard}
      </FlexBox>
    )
  }

  private get titlePreposition(): string {
    const {title} = this.props
    switch (title.toLowerCase()) {
      case 'read':
        return 'from'
      case 'write':
        return 'to'
      default:
        return 'on'
    }
  }

  private get bucketTabs(): BucketTab[] {
    return [BucketTab.AllBuckets, BucketTab.Scoped]
  }

  private get builderCard(): JSX.Element {
    const {
      selectedBuckets,
      onSelect,
      onSelectAll,
      onDeselectAll,
      buckets,
      activeTab,
      title,
    } = this.props

    switch (activeTab) {
      case BucketTab.AllBuckets:
        return (
          <>
            <EmptyState size={ComponentSize.Small}>
              <EmptyState.Text>
                This token will be able to {title.toLowerCase()}{' '}
                {this.titlePreposition} all existing buckets as well as{' '}
                {this.titlePreposition} any bucket created in the future
              </EmptyState.Text>
            </EmptyState>
          </>
        )
      case BucketTab.Scoped:
        return (
          <SelectorList className="bucket-selectors">
            <SelectorList.Header title="Buckets">
              <div className="bucket-selectors--buttons">
                <FlexBox
                  alignItems={AlignItems.Center}
                  direction={FlexDirection.Row}
                  margin={ComponentSize.Small}
                >
                  <Button
                    text="Select All"
                    size={ComponentSize.ExtraSmall}
                    className="bucket-selectors--button"
                    onClick={onSelectAll}
                  />
                  <Button
                    text="Deselect All"
                    size={ComponentSize.ExtraSmall}
                    className="bucket-selectors--button"
                    onClick={onDeselectAll}
                  />
                </FlexBox>
              </div>
            </SelectorList.Header>
            <BucketsTabBody
              buckets={buckets}
              onSelect={onSelect}
              selectedBuckets={selectedBuckets}
            />
          </SelectorList>
        )
    }
  }
}

export default BucketsSelector
