// Libraries
import React, {FC, useEffect, useRef, useState} from 'react'
import moment from 'moment'
import {connect} from 'react-redux'
import {
  Columns,
  Form,
  Grid,
  Panel,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  PopoverType,
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
import TableGraph from 'src/shared/components/tables/TableGraph'
import TimeRangeDropdown from 'src/shared/components/DeleteDataForm/TimeRangeDropdown'

// Types
import {
  AppState,
  Filter,
  RemoteDataState,
  TableViewProperties,
  TimeZone,
} from 'src/types'

// Selectors
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {setCanDelete} from 'src/shared/selectors/canDelete'
import {
  getXColumnSelection,
  getYColumnSelection,
  getFillColumnsSelection,
  getSymbolColumnsSelection,
} from 'src/timeMachine/selectors'

// Actions
import {
  deleteFilter,
  deleteWithPredicate,
  resetFilters,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
} from 'src/shared/actions/predicates'
import {executePreviewQuery} from 'src/timeMachine/actions/queries'

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
  properties: TableViewProperties
  timeRange: [number, number]
  timeZone: TimeZone
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
  properties,
  setDeletionStatus,
  setFilter,
  setIsSerious,
  setBucketAndKeys,
  setTimeRange,
  timeRange,
  timeZone,
  values,
}) => {
  const name = bucketName || initialBucketName
  const [count, setCount] = useState(0)
  const checkboxRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    // trigger the setBucketAndKeys if the bucketName hasn't been set
    if (bucketName === '' && name !== undefined) {
      setBucketAndKeys(orgID, name)
    }
  })

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

    await executePreviewQuery(rowQuery) // const variableAssignments = getVariableAssignments(getState())
    setCount(Number(total))
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
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Panel className="delete-data-form--danger-zone">
              <Panel.Header>
                <Panel.Title>Danger Zone!</Panel.Title>
              </Panel.Header>
              <Panel.Body className="delete-data-form--confirm">
                <div ref={checkboxRef} onMouseOver={handleDeleteDataPreview}>
                  <Checkbox
                    testID="delete-checkbox"
                    label="I understand that this cannot be undone."
                    checked={isSerious}
                    onSetChecked={isSerious => setIsSerious(isSerious)}
                  />
                </div>
                <DeleteButton
                  status={deletionStatus}
                  valid={canDelete}
                  onClick={handleDelete}
                />
              </Panel.Body>
            </Panel>
          </Grid.Column>
        </Grid.Row>
        <Popover
          type={PopoverType.Outline}
          position={PopoverPosition.ToTheLeft}
          triggerRef={checkboxRef}
          showEvent={PopoverInteraction.Hover}
          hideEvent={PopoverInteraction.Hover}
          distanceFromTrigger={8}
          testID="checkbox-popover"
          contents={() => {
            if (count > 0 && files && files.length > 0) {
              return (
                <>
                  Amount of data to be deleted: {count}
                  <FluxTablesTransform files={files}>
                    {tables => (
                      <TableGraph
                        table={tables[0]}
                        properties={properties}
                        timeZone={timeZone}
                      />
                    )}
                  </FluxTablesTransform>
                </>
              )
            }
            return <>No data to preview</>
          }}
        />
      </Grid>
    </Form>
  )
}

const mstp = (state: AppState) => {
  const {
    predicates,
    predicates: {
      bucketName,
      deletionStatus,
      filters,
      keys,
      isSerious,
      timeRange,
      values,
    },
  } = state

  const {
    queryResults: {files},
    view: {properties: viewProperties},
  } = getActiveTimeMachine(state)

  const timeZone = state.app.persisted.timeZone
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)
  const symbolColumns = getSymbolColumnsSelection(state)
  const properties = {
    ...viewProperties,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
  }

  console.log('properties: ', properties)

  return {
    bucketName,
    canDelete: setCanDelete(predicates),
    deletionStatus,
    files,
    filters,
    isSerious,
    keys,
    properties,
    timeRange,
    timeZone,
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
