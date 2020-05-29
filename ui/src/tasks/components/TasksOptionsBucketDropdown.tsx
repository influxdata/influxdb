// Libraries
import React, {PureComponent} from 'react'
import {get} from 'lodash'
import {connect} from 'react-redux'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

// Utils
import {isSystemBucket} from 'src/buckets/constants/index'
import {getAll, getStatus} from 'src/resources/selectors'

// Types
import {RemoteDataState, AppState, Bucket, ResourceType} from 'src/types'

interface OwnProps {
  onChangeBucketName: (selectedBucketName: string) => void
  selectedBucketName: string
}

interface StateProps {
  buckets: Bucket[]
  status: RemoteDataState
}

type Props = OwnProps & StateProps

class TaskOptionsBucketDropdown extends PureComponent<Props> {
  public componentDidMount() {
    this.setSelectedToFirst()
  }

  public componentDidUpdate(prevProps: Props) {
    if (this.props.buckets !== prevProps.buckets) {
      this.setSelectedToFirst()
    }
  }

  public render() {
    const {selectedBucketName} = this.props

    return (
      <Dropdown
        button={(active, onClick) => (
          <Dropdown.Button
            active={active}
            onClick={onClick}
            status={this.status}
            testID="task-options-bucket-dropdown--button"
          >
            {selectedBucketName}
          </Dropdown.Button>
        )}
        menu={onCollapse => (
          <Dropdown.Menu onCollapse={onCollapse}>
            {this.dropdownItems}
          </Dropdown.Menu>
        )}
      />
    )
  }

  private get dropdownItems(): JSX.Element[] {
    const {buckets} = this.props

    if (!buckets || !buckets.length) {
      return [
        <Dropdown.Item id="no-buckets" key="no-buckets" value="no-buckets">
          No Buckets found in Org
        </Dropdown.Item>,
      ]
    }

    const nonSystemBuckets = buckets.filter(
      bucket => !isSystemBucket(bucket.name)
    )

    return nonSystemBuckets.map(bucket => {
      return (
        <Dropdown.Item
          id={bucket.name}
          key={bucket.name}
          value={bucket.name}
          onClick={this.props.onChangeBucketName}
          selected={bucket.name === this.selectedName}
        >
          {bucket.name}
        </Dropdown.Item>
      )
    })
  }

  private get status(): ComponentStatus {
    const {status, buckets} = this.props
    if (status === RemoteDataState.Loading) {
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
      return get(buckets, '0.name', '')
    }
    return 'no-buckets'
  }

  private setSelectedToFirst() {
    const {buckets, selectedBucketName, onChangeBucketName} = this.props
    const firstBucketNameInList = get(buckets, '0.name', '')

    if (selectedBucketName) {
      return
    }

    onChangeBucketName(firstBucketNameInList)
  }
}

const mstp = (state: AppState): StateProps => {
  const buckets = getAll<Bucket>(state, ResourceType.Buckets).filter(
    (bucket: Bucket): boolean => bucket.type !== 'system'
  )
  const status = getStatus(state, ResourceType.Buckets)

  return {
    buckets,
    status,
  }
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TaskOptionsBucketDropdown)
