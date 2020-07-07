// Libraries
import React, {FC, useEffect, useState} from 'react'
import moment from 'moment'
import {connect, ConnectedProps} from 'react-redux'
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
import {Filter, AppState} from 'src/types'

// Selectors
import {setCanDelete} from 'src/shared/selectors/canDelete'
import {getOrg} from 'src/organizations/selectors'

// Actions
import {
  deleteFilter,
  deleteWithPredicate,
  executePreviewQuery,
  resetFilters,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
} from 'src/shared/actions/predicates'

interface OwnProps {
  handleDismiss: () => void
}

type ReduxProps = ConnectedProps<typeof connector>
export type Props = ReduxProps & OwnProps

const DeleteDataForm: FC<Props> = ({
  canDelete,
  deleteFilter,
  deletionStatus,
  deleteWithPredicate,
  executePreviewQuery,
  files,
  filters,
  handleDismiss,
  isSerious,
  keys,
  resetFilters,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
  timeRange,
  values,
  bucketName,
  orgID,
}) => {
  const [count, setCount] = useState('0')
  useEffect(() => {
    // trigger the setBucketAndKeys if the bucketName hasn't been set
    if (bucketName) {
      setBucketAndKeys(bucketName)
    }
  })

  useEffect(() => {
    if (filters.every(filter => filter.key !== '' && filter.value !== '')) {
      handleDeleteDataPreview()
    }
  }, [filters])

  const formatPredicatesForPreview = (predicates: Filter[]) => {
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
    const {lower, upper} = timeRange

    let query = `from(bucket: "${name}")
      |> range(start: ${moment(lower).toISOString()}, stop: ${moment(
      upper
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
    deleteWithPredicate()
    handleDismiss()
  }

  const handleBucketClick = (selectedBucketName: string) => {
    setBucketAndKeys(selectedBucketName)
    resetFilters()
  }

  const formatNumber = (num: string) => {
    if (num) {
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
                bucketName={bucketName}
                onSetBucketName={handleBucketClick}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight}>
            <Form.Element label="Time Range">
              <TimeRangeDropdown
                timeRange={timeRange}
                onSetTimeRange={setTimeRange}
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
              onDeleteFilter={deleteFilter}
              onSetFilter={setFilter}
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
                <h5>
                  Danger Zone! You're deleting{' '}
                  <span style={{color: InfluxColors.Dreamsicle}}>
                    {formatNumber(count)}
                  </span>{' '}
                  records
                </h5>
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

const mstp = (state: AppState) => {
  const {predicates} = state
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
    orgID: getOrg(state).id,
  }
}

const mdtp = {
  deleteFilter,
  deleteWithPredicate,
  resetFilters,
  executePreviewQuery,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
}

const connector = connect(mstp, mdtp)

export default connector(DeleteDataForm)
