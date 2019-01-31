// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import FilterList from 'src/shared/components/Filter'
import BucketList from 'src/organizations/components/BucketList'
import {PrettyBucket} from 'src/organizations/components/BucketRow'
import CreateBucketOverlay from 'src/organizations/components/CreateBucketOverlay'
import {
  Input,
  Button,
  ComponentColor,
  IconFont,
  OverlayTechnology,
  ComponentSize,
  EmptyState,
} from 'src/clockface'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Utils
import {ruleToString} from 'src/utils/formatting'

// APIs
import {client} from 'src/utils/api'

// Types
import {OverlayState} from 'src/types/v2'

import {Bucket, Organization, BucketRetentionRules} from 'src/api'

interface Props {
  org: Organization
  buckets: Bucket[]
  onChange: () => void
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface State {
  buckets: PrettyBucket[]
  searchTerm: string
  overlayState: OverlayState
}

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {bucketDeleted} from 'src/shared/copy/v2/notifications'

@ErrorHandling
export default class Buckets extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      buckets: this.prettyBuckets(this.props.buckets),
    }
  }

  public render() {
    const {org, buckets} = this.props
    const {searchTerm, overlayState} = this.state

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter Buckets..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          <Button
            text="Create Bucket"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
          />
        </TabbedPageHeader>
        <FilterList<PrettyBucket>
          searchTerm={searchTerm}
          searchKeys={['name', 'ruleString']}
          list={this.prettyBuckets(buckets)}
        >
          {bs => (
            <BucketList
              buckets={bs}
              emptyState={this.emptyState}
              onUpdateBucket={this.handleUpdateBucket}
              onDeleteBucket={this.handleDeleteBucket}
            />
          )}
        </FilterList>
        <OverlayTechnology visible={overlayState === OverlayState.Open}>
          <CreateBucketOverlay
            org={org}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
        </OverlayTechnology>
      </>
    )
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    await client.buckets.update(updatedBucket.id, updatedBucket)
    this.props.onChange()
  }

  private handleDeleteBucket = async (deletedBucket: PrettyBucket) => {
    const {onChange, notify} = this.props
    await client.buckets.delete(deletedBucket.id)
    onChange()
    notify(bucketDeleted(deletedBucket.name))
  }

  private handleCreateBucket = async (
    org: Organization,
    bucket: Bucket
  ): Promise<void> => {
    await client.buckets.create(org, bucket)
    this.props.onChange()
    this.handleCloseModal()
  }

  private handleOpenModal = (): void => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({overlayState: OverlayState.Closed})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private prettyBuckets(buckets: Bucket[]): PrettyBucket[] {
    return buckets.map(b => {
      const expire = b.retentionRules.find(
        rule => rule.type === BucketRetentionRules.TypeEnum.Expire
      )

      if (!expire) {
        return {
          ...b,
          ruleString: 'forever',
        }
      }

      return {
        ...b,
        ruleString: ruleToString(expire.everySeconds),
      }
    })
  }

  private get emptyState(): JSX.Element {
    const {org} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${org.name} does not own any Buckets , why not create one?`}
            highlightWords={['Buckets']}
          />
          <Button
            text="Create Bucket"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenModal}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Buckets match your query" />
      </EmptyState>
    )
  }
}
