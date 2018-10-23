// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import BucketOverlay from 'src/organizations/components/BucketOverlay'
import FilterList from 'src/organizations/components/Filter'
import BucketList, {PrettyBucket} from 'src/organizations/components/BucketList'

import {
  Input,
  Button,
  ComponentColor,
  IconFont,
  OverlayTechnology,
  ComponentSize,
  EmptyState,
} from 'src/clockface'

// Utils
import {rpToString} from 'src/utils/formatting'

// APIs
import {createBucket} from 'src/organizations/apis'

// Types
import {Bucket, Organization} from 'src/types/v2'

interface Props {
  org: Organization
  buckets: Bucket[]
}

interface State {
  buckets: Bucket[]
  searchTerm: string
  modalState: ModalState
}

enum ModalState {
  Open = 'open',
  Closed = 'closed',
}

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
export default class Buckets extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      buckets: this.props.buckets,
      modalState: ModalState.Closed,
    }
  }

  public render() {
    const {org} = this.props
    const {searchTerm, modalState} = this.state

    return (
      <>
        <ProfilePage.Header>
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
        </ProfilePage.Header>
        <FilterList<PrettyBucket>
          searchTerm={searchTerm}
          searchKeys={['name', 'retentionPeriod']}
          list={this.prettyBuckets}
        >
          {bs => <BucketList buckets={bs} emptyState={this.emptyState} />}
        </FilterList>
        <OverlayTechnology visible={modalState === ModalState.Open}>
          <BucketOverlay
            link={org.links.buckets}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
        </OverlayTechnology>
      </>
    )
  }

  private handleCreateBucket = async (
    link: string,
    bucket: Partial<Bucket>
  ): Promise<void> => {
    const {buckets} = this.state
    const b = await createBucket(link, bucket)
    this.setState({buckets: [b, ...buckets]})
    this.handleCloseModal()
  }

  private handleOpenModal = (): void => {
    this.setState({modalState: ModalState.Open})
  }

  private handleCloseModal = (): void => {
    this.setState({modalState: ModalState.Closed})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get prettyBuckets(): PrettyBucket[] {
    return this.props.buckets.map(b => ({
      ...b,
      retentionPeriod: rpToString(231432430000),
    }))
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text text="Oh noes I dun see na buckets" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="No buckets match your query" />
      </EmptyState>
    )
  }
}
