// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import FilterList from 'src/shared/components/Filter'
import BucketList from 'src/configuration/components/BucketList'
import {PrettyBucket} from 'src/organizations/components/BucketRow'
import CreateBucketOverlay from 'src/organizations/components/CreateBucketOverlay'

import {
  ComponentSize,
  Button,
  ComponentColor,
  IconFont,
} from '@influxdata/clockface'
import {Input, OverlayTechnology, EmptyState, Tabs} from 'src/clockface'

// Utils
import {ruleToString} from 'src/utils/formatting'

// Types
import {OverlayState} from 'src/types'
import {AppState} from 'src/types/v2'

import {Bucket, BucketRetentionRules} from '@influxdata/influx'

interface StateProps {
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
class Buckets extends PureComponent<StateProps, State> {
  constructor(props: StateProps) {
    super(props)

    this.state = {
      searchTerm: '',
      overlayState: OverlayState.Closed,
      buckets: this.prettyBuckets(this.props.buckets),
    }
  }

  public render() {
    const {buckets} = this.props
    const {searchTerm} = this.state

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
        {/*<OverlayTechnology visible={overlayState === OverlayState.Open}>
          <CreateBucketOverlay
            org={org}
            onCloseModal={this.handleCloseModal}
            onCreateBucket={this.handleCreateBucket}
          />
          </OverlayTechnology>*/}
      </>
    )
  }

  private handleUpdateBucket = async (updatedBucket: PrettyBucket) => {
    // const {onChange, notify} = this.props
    // try {
    //   await client.buckets.update(updatedBucket.id, updatedBucket)
    //   onChange()
    //   notify(bucketUpdateSuccess(updatedBucket.name))
    // } catch (e) {
    //   console.error(e)
    //   notify(bucketUpdateFailed(updatedBucket.name))
    // }
  }

  private handleDeleteBucket = async (deletedBucket: PrettyBucket) => {
    // const {onChange, notify} = this.props
    // try {
    //   await client.buckets.delete(deletedBucket.id)
    //   onChange()
    //   notify(bucketDeleteSuccess(deletedBucket.name))
    // } catch (e) {
    //   console.error(e)
    //   bucketDeleteFailed(deletedBucket.name)
    // }
  }

  private handleCreateBucket = async (bucket: Bucket): Promise<void> => {
    // const {onChange, notify} = this.props
    // try {
    //   await client.buckets.create(bucket)
    //   onChange()
    //   this.handleCloseModal()
    //   notify(bucketCreateSuccess())
    // } catch (e) {
    //   console.error(e)
    //   notify(bucketCreateFailed())
    // }
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

const mstp = ({buckets}: AppState): StateProps => {
  return {
    buckets: buckets.list,
  }
}

export default connect<StateProps>(
  mstp,
  null
)(Buckets)
