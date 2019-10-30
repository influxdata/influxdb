// Libraries
import React, {FunctionComponent} from 'react'
import moment from 'moment'
import {connect} from 'react-redux'
import {Form, Grid, Columns, Panel} from '@influxdata/clockface'

// Components
import BucketsDropdown from 'src/shared/components/DeleteDataForm/BucketsDropdown'
import TimeRangeDropdown from 'src/shared/components/DeleteDataForm/TimeRangeDropdown'
import Checkbox from 'src/shared/components/Checkbox'
import DeleteButton from 'src/shared/components/DeleteDataForm/DeleteButton'
import FilterEditor from 'src/shared/components/DeleteDataForm/FilterEditor'

// Types
import {RemoteDataState} from 'src/types'

// action
import {
  deleteFilter,
  deleteWithPredicate,
  setBucketName,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setTimeRange,
} from 'src/shared/actions/predicates'

// state
// import {PredicatesState} from 'src/shared/reducers/predicates'

interface OwnProps {
  orgID: string
  handleDismiss: () => void
  initialBucketName?: string
  initialTimeRange?: [number, number]
}

interface StateProps {
  filters: Array<{key?: string, equality?: string, value?: string}>
  timeRange: Array<string | number>
  isSerious: boolean
  deletionStatus: RemoteDataState
}

interface DispatchProps {
  deleteFilter: typeof deleteFilter
  deleteWithPredicate: typeof deleteWithPredicate
  setBucketName: typeof setBucketName
  setDeletionStatus: typeof setDeletionStatus
  setFilter: typeof setFilter
  setIsSerious: typeof setIsSerious
  setTimeRange: typeof setTimeRange
}

type Props = StateProps & DispatchProps & OwnProps

const DeleteDataForm: FunctionComponent<Props> = ({
  bucketName,
  deleteFilter,
  deletionStatus,
  deleteWithPredicate,
  filters,
  handleDismiss,
  initialBucketName,
  initialTimeRange,
  isSerious,
  orgID,
  setBucketName,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setTimeRange,
  timeRange,
}) => {

  const name = bucketName || initialBucketName

  const realTimeRange = initialTimeRange || timeRange

  const canDelete =
    isSerious &&
    deletionStatus === RemoteDataState.NotStarted &&
    filters.every(f => !!f.key && !!f.value && !!f.equality)

  const formatPredicates = predicates => {
    const result = [];
    predicates.forEach(predicate => {
      const {key, equality, value} = predicate
      result.push(`${key} ${equality} ${value}`)
    })
    return result.join(' AND ')
  }

  const handleDelete = async () => {
    setDeletionStatus(RemoteDataState.Loading)

    const [start, stop] = realTimeRange

    const data = {
      start: moment(start).toISOString(),
      stop: moment(stop).toISOString(),
    }

    if (filters.length > 0) {
      data.predicate = formatPredicates(filters)
    }

    const params = {
      data,
      query: {
        orgID,
        bucket: name,
      },
    }

    deleteWithPredicate(params)
    handleDismiss()
  }

  return (
    <Form className="delete-data-form">
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Four}>
            <Form.Element label="Target Bucket">
              <BucketsDropdown
                bucketName={name}
                onSetBucketName={bucketName => setBucketName(bucketName)}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight}>
            <Form.Element label="Time Range">
              <TimeRangeDropdown
                timeRange={realTimeRange}
                onSetTimeRange={timeRange => setTimeRange(timeRange)}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <FilterEditor
              filters={filters}
              onSetFilter={(filter, index) => setFilter(filter, index)}
              onDeleteFilter={index => deleteFilter(index)}
              shouldValidate={isSerious}
            />
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Panel className="delete-data-form--danger-zone">
              <Panel.Header>
                <Panel.Title>Danger Zone!</Panel.Title>
              </Panel.Header>
              <Panel.Body className="delete-data-form--confirm">
                <Checkbox
                  label="I understand that this cannot be undone."
                  checked={isSerious}
                  onSetChecked={isSerious => setIsSerious(isSerious)}
                />
                <DeleteButton
                  status={deletionStatus}
                  valid={canDelete}
                  onClick={handleDelete}
                />
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    </Form>
  )
}

const mstp = (state: PredicatesState) => {
  const {
    predicates: {
      bucketName,
      deletionStatus,
      filters,
      isSerious,
      timeRange,
    }
  } = state

  return {
    bucketName,
    deletionStatus,
    filters,
    isSerious,
    timeRange,
  }
}

const mdtp = {
  deleteFilter,
  deleteWithPredicate,
  setBucketName,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setTimeRange,
}

export default connect<StateProps, DispatchProps, {}>(mstp, mdtp)(DeleteDataForm)
