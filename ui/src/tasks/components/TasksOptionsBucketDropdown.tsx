// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Dropdown, ComponentStatus} from 'src/clockface'

// Types
import {Bucket} from 'src/api'
import {RemoteDataState} from 'src/types'

interface Props {
  buckets: Bucket[]
  onChangeBucketName: (selectedBucketName: string) => void
  selectedBucketName: string
  loading: RemoteDataState
}

export default class TaskOptionsBucketDropdown extends PureComponent<Props> {
  public componentDidMount() {
    this.setSelectedToFirst()
  }
  public componentDidUpdate(prevProps: Props) {
    if (this.props.buckets !== prevProps.buckets) {
      this.setSelectedToFirst()
    }
  }
  public render() {
    return (
      <Dropdown
        selectedID={this.selectedName}
        onChange={this.props.onChangeBucketName}
        status={this.status}
      >
        {this.dropdownItems}
      </Dropdown>
    )
  }

  private get dropdownItems(): JSX.Element[] {
    const {buckets} = this.props

    if (buckets && buckets.length) {
      return buckets.map(bucket => {
        return (
          <Dropdown.Item id={bucket.name} key={bucket.name} value={bucket.name}>
            {bucket.name}
          </Dropdown.Item>
        )
      })
    } else {
      return [
        <Dropdown.Item
          id={'no-buckets'}
          key={'no-buckets'}
          value={'no-buckets'}
        >
          {'no buckets found in org'}
        </Dropdown.Item>,
      ]
    }
  }
  private get status(): ComponentStatus {
    const {loading, buckets} = this.props
    if (loading === RemoteDataState.Loading) {
      return ComponentStatus.Loading
    }
    if (!buckets || !buckets.length) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private get selectedName(): string {
    const {selectedBucketName, buckets} = this.props
    if (buckets && buckets.length) {
      if (selectedBucketName) {
        return selectedBucketName
      }
      return _.get(buckets, '0.name', '')
    }
    return 'no-buckets'
  }

  private setSelectedToFirst() {
    const {buckets, onChangeBucketName} = this.props
    const firstBucketNameInList = _.get(buckets, '0.name', '')

    onChangeBucketName(firstBucketNameInList)
  }
}
