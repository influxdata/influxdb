// Libraries
import React, {FC, useEffect, useState} from 'react'
import moment from 'moment'
import {connect} from 'react-redux'
import {
  Columns,
  ComponentSize,
  Form,
  Grid,
  InfluxColors,
  Panel,
} from '@influxdata/clockface'
import {extractBoxedCol} from 'src/timeMachine/apis/queryBuilder'

// Utils
import {runQuery} from 'src/shared/apis/query'

// Components
import BucketsDropdown from 'src/shared/components/DeleteDataForm/BucketsDropdown'
import Checkbox from 'src/shared/components/Checkbox'
import DeleteButton from 'src/shared/components/DeleteDataForm/DeleteButton'
import FilterEditor from 'src/shared/components/DeleteDataForm/FilterEditor'
import FluxTablesTransform from 'src/shared/components/FluxTablesTransform'
import PreviewDataTable from 'src/shared/components/DeleteDataForm/PreviewDataTable'
import TimeRangeDropdown from 'src/shared/components/DeleteDataForm/TimeRangeDropdown'

// Types
import {AppState, Filter, RemoteDataState} from 'src/types'

// Selectors
import {setCanDelete} from 'src/shared/selectors/canDelete'

// Actions
import {
  deleteFilter,
  deleteWithPredicate,
  executePreviewQuery,
  resetFilters,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
} from 'src/shared/actions/predicates'

interface OwnProps {
  orgID: string
  handleDismiss: () => void
  initialBucketName?: string
  initialTimeRange?: [number, number]
  keys: string[]
  values: (string | number)[]
}

interface StateProps {
  bucketName: string
  canDelete: boolean
  deletionStatus: RemoteDataState
  files: string[]
  filters: Filter[]
  isSerious: boolean
  keys: string[]
  timeRange: [number, number]
  values: (string | number)[]
}

interface DispatchProps {
  deleteFilter: (index: number) => void
  deleteWithPredicate: typeof deleteWithPredicate
  resetFilters: () => void
  executePreviewQuery: typeof executePreviewQuery
  setDeletionStatus: typeof setDeletionStatus
  setFilter: typeof setFilter
  setIsSerious: (isSerious: boolean) => void
  setBucketAndKeys: (orgID: string, bucketName: string) => void
  setTimeRange: (timeRange: [number, number]) => void
}

export type Props = StateProps & DispatchProps & OwnProps

const DeleteDataForm: FC<Props> = ({
  bucketName,
  canDelete,
  deleteFilter,
  deletionStatus,
  deleteWithPredicate,
  executePreviewQuery,
  files,
  filters,
  handleDismiss,
  initialBucketName,
  initialTimeRange,
  isSerious,
  keys,
  orgID,
  resetFilters,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
  timeRange,
  values,
}) => {
  const name = bucketName || initialBucketName
  const [count, setCount] = useState('0')
  useEffect(() => {
    // trigger the setBucketAndKeys if the bucketName hasn't been set
    if (bucketName === '' && name !== undefined) {
      setBucketAndKeys(orgID, name)
    }
  })

  useEffect(() => {
    if (filters.every(filter => filter.key !== '' && filter.value !== '')) {
      handleDeleteDataPreview()
    }
  }, [filters])

  const realTimeRange = initialTimeRange || timeRange

  const formatPredicatesForDeletion = predicates => {
    const result = []
    predicates.forEach(predicate => {
      const {key, equality, value} = predicate
      result.push(`${key} ${equality} ${value}`)
    })
    return result.join(' AND ')
  }

  const formatPredicatesForPreview = predicates => {
    let result = ''
    predicates.forEach(predicate => {
      const {key, equality, value} = predicate
      result += `\n|> filter(fn: (r) => r.${key} ${
        equality === '=' ? '==' : '!='
      } "${value}")`
    })
    return result
  }

  const handleDeleteDataPreview = async () => {
    const [start, stop] = realTimeRange

    let query = `from(bucket: "${name}")
      |> range(start: ${moment(start).toISOString()}, stop: ${moment(
      stop
    ).toISOString()})`

    if (filters.length > 0) {
      query += ` ${formatPredicatesForPreview(filters)}`
    }

    const countQuery = `${query}
      |> count()
      |> keep(columns: ["_value"])
      |> sum()
    `

    const rowQuery = `${query}
      |> limit(n: 1)
      |> yield(name: "sample_data")
    `
    const [total] = await extractBoxedCol(runQuery(orgID, countQuery), '_value')
      .promise
    executePreviewQuery(rowQuery)
    setCount(total)
  }

  const handleDelete = () => {
    setDeletionStatus(RemoteDataState.Loading)

    const [start, stop] = realTimeRange

    const data = {
      start: moment(start).toISOString(),
      stop: moment(stop).toISOString(),
    }

    if (filters.length > 0) {
      data['predicate'] = formatPredicatesForDeletion(filters)
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

  const handleBucketClick = selectedBucket => {
    setBucketAndKeys(orgID, selectedBucket)
    resetFilters()
  }

  const formatNumber = num => {
    if (num !== undefined) {
      return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')
    }
    return 0
  }

  return (
    <Form className="delete-data-form">
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Four}>
            <Form.Element label="Target Bucket">
              <BucketsDropdown
                bucketName={name}
                onSetBucketName={bucketName => handleBucketClick(bucketName)}
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
              bucket={name}
              filters={filters}
              keys={keys}
              onDeleteFilter={index => deleteFilter(index)}
              onSetFilter={(filter, index) => setFilter(filter, index)}
              orgID={orgID}
              shouldValidate={isSerious}
              values={values}
            />
          </Grid.Column>
        </Grid.Row>
        <Grid.Row className="delete-data-preview">
          <Grid.Column widthXS={Columns.Twelve}>
            <Panel>
              <Panel.Header size={ComponentSize.ExtraSmall}>
                <p className="preview-data-margins">Preview Data</p>
              </Panel.Header>
              <Panel.Body size={ComponentSize.ExtraSmall}>
                {files && files.length > 0 && files[0].length > 1 && (
                  <FluxTablesTransform files={files}>
                    {tables => {
                      const [table] = tables
                      if (table && table.data) {
                        let [headers, bodyData] = table.data
                        headers = headers.slice(3)
                        bodyData = bodyData.slice(3)
                        return (
                          <PreviewDataTable
                            headers={headers}
                            bodyData={bodyData}
                          />
                        )
                      }
                      return <span />
                    }}
                  </FluxTablesTransform>
                )}
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Panel className="delete-data-form--danger-zone">
              <Panel.Header>
                <Panel.Title>
                  Danger Zone! You're deleting{' '}
                  <span style={{color: InfluxColors.Dreamsicle}}>
                    {formatNumber(count)}
                  </span>{' '}
                  records
                </Panel.Title>
              </Panel.Header>
              <Panel.Body className="delete-data-form--confirm">
                <Checkbox
                  testID="delete-checkbox"
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

const mstp = ({predicates}: AppState) => {
  const {
    bucketName,
    deletionStatus,
    files,
    filters,
    keys,
    isSerious,
    timeRange,
    values,
  } = predicates

  return {
    bucketName,
    canDelete: setCanDelete(predicates),
    deletionStatus,
    files,
    filters,
    isSerious,
    keys,
    timeRange,
    values,
  }
}

const mdtp = {
  deleteFilter,
  deleteWithPredicate,
  resetFilters,
  executePreviewQuery,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(DeleteDataForm)
