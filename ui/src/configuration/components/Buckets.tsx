// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import FilterList from 'src/shared/components/Filter'
import BucketList from 'src/configuration/components/BucketList'
import {PrettyBucket} from 'src/organizations/components/BucketRow'
import CreateBucketOverlay from 'src/configuration/components/CreateBucketOverlay'

import {
  ComponentSize,
  Button,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import {Input, Overlay, EmptyState, Tabs} from 'src/clockface'

// Actions
import {createBucket, updateBucket, deleteBucket} from 'src/buckets/actions'

// Utils
import {ruleToString} from 'src/utils/formatting'

// Types
import {OverlayState} from 'src/types'
import {AppState} from 'src/types/v2'

import {Bucket, BucketRetentionRules, Organization} from '@influxdata/influx'

interface StateProps {
  orgs: Organization[]
  buckets: Bucket[]
}

interface DispatchProps {
  createBucket: typeof createBucket
  updateBucket: typeof updateBucket
  deleteBucket: typeof deleteBucket
}

interface State {
  buckets: PrettyBucket[]
  searchTerm: string
  overlayState: OverlayState
}

type Props = DispatchProps & StateProps

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class Buckets extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      buckets: this.prettyBuckets(this.props.buckets),
    }
  }

  public render() {
    const {buckets, orgs} = this.props
    const {searchTerm, overlayState} = this.state

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter buckets..."
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
        </Tabs.TabContentsHeader>
        <FilterList<PrettyBucket>
          searchTerm={searchTerm}
          searchKeys={['name', 'ruleString', 'organization', 'labels[].name']}
          list={this.prettyBuckets(buckets)}
        >
          {bs => (
            <BucketList
              buckets={bs}
              emptyState={this.emptyState}
              onUpdateBucket={this.handleUpdateBucket}
              onDeleteBucket={this.handleDeleteBucket}
              onFilterChange={this.handleFilterUpdate}
            />
          )}
        </FilterList>
        <Overlay visible={overlayState === OverlayState.Open}>
          <CreateBucketOverlay
            orgs={orgs}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
        </Overlay>
      </>
    )
  }

  private handleUpdateBucket = (updatedBucket: PrettyBucket) => {
    this.props.updateBucket(updatedBucket as Bucket)
  }

  private handleDeleteBucket = ({id, name}: PrettyBucket) => {
    this.props.deleteBucket(id, name)
  }

  private handleCreateBucket = async (bucket: Bucket): Promise<void> => {
    try {
      await this.props.createBucket(bucket)
      this.handleCloseModal()
    } catch (e) {}
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
    this.handleFilterUpdate(e.target.value)
  }

  private handleFilterUpdate = (searchTerm: string): void => {
    this.setState({searchTerm})
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
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text
            text="Looks like your don't own any Buckets , why not create one?"
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
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="No Buckets match your query" />
      </EmptyState>
    )
  }
}

const mstp = ({buckets, orgs}: AppState): StateProps => {
  return {
    buckets: buckets.list,
    orgs,
  }
}

const mdtp = {
  createBucket,
  updateBucket,
  deleteBucket,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Buckets)
