// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

// Types
import {RemoteDataState, AppState, Bucket} from 'src/types'

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

    if (buckets && buckets.length) {
      return buckets.map(bucket => {
        return (
          <Dropdown.Item
            id={bucket.name}
            key={bucket.name}
            value={bucket.name}
            onClick={this.props.onChangeBucketName}
            selected={bucket.id === this.selectedName}
          >
            {bucket.name}
          </Dropdown.Item>
        )
      })
    } else {
      return [
        <Dropdown.Item id="no-buckets" key="no-buckets" value="no-buckets">
          No Buckets found in Org
        </Dropdown.Item>,
      ]
    }
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

const mstp = ({buckets}: AppState): StateProps => {
  return {
    buckets: buckets.list,
    status: buckets.status,
  }
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TaskOptionsBucketDropdown)
