// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import ProfilePage from 'src/shared/components/profile_page/ProfilePage'
import FilterList from 'src/organizations/components/Filter'
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

// Utils
import {ruleToString} from 'src/utils/formatting'

// APIs
import {createBucket, updateBucket} from 'src/organizations/apis'

// Types
import {
  Bucket,
  Organization,
  RetentionRuleTypes,
  OverlayState,
} from 'src/types/v2'

interface Props {
  org: Organization
  buckets: Bucket[]
}

interface State {
  buckets: PrettyBucket[]
  searchTerm: string
  overlayState: OverlayState
}

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

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
    const {org} = this.props
    const {searchTerm, overlayState} = this.state

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
          searchKeys={['name', 'ruleString']}
          list={this.prettyBuckets(this.state.buckets)}
        >
          {bs => (
            <BucketList
              buckets={bs}
              emptyState={this.emptyState}
              onUpdateBucket={this.handleUpdateBucket}
            />
          )}
        </FilterList>
        <OverlayTechnology visible={overlayState === OverlayState.Open}>
          <CreateBucketOverlay
            org={org}
            link={org.links.buckets}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
        </OverlayTechnology>
      </>
    )
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    const bucket = await updateBucket(updatedBucket)
    const buckets = this.state.buckets.map(b => {
      if (b.id === bucket.id) {
        return bucket
      }

      return b
    })

    this.setState({buckets: this.prettyBuckets(buckets)})
  }

  private handleCreateBucket = async (
    link: string,
    bucket: Partial<Bucket>
  ): Promise<void> => {
    const b = await createBucket(link, bucket)
    const buckets = this.prettyBuckets([b, ...this.props.buckets])
    this.setState({buckets})
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
        rule => rule.type === RetentionRuleTypes.Expire
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
