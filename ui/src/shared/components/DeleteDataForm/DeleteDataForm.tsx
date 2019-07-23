// Libraries
import React, {useReducer, FunctionComponent} from 'react'
import moment from 'moment'
import {Form, Grid, Columns, Panel} from '@influxdata/clockface'

// Components
import BucketsDropdown from 'src/shared/components/DeleteDataForm/BucketsDropdown'
import TimeRangeDropdown from 'src/shared/components/DeleteDataForm/TimeRangeDropdown'
import Checkbox from 'src/shared/components/Checkbox'
import DeleteButton from 'src/shared/components/DeleteDataForm/DeleteButton'
import FilterEditor, {
  Filter,
} from 'src/shared/components/DeleteDataForm/FilterEditor'

// Types
import {RemoteDataState} from 'src/types'

const HOUR_MS = 1000 * 60 * 60

const FAKE_API_CALL = async (_: any) => {
  await new Promise(res => setTimeout(res, 2000))
}

interface Props {
  orgID: string
  initialBucketName?: string
  initialTimeRange?: [number, number]
}

interface State {
  bucketName: string
  timeRange: [number, number]
  filters: Filter[]
  isSerious: boolean
  deletionStatus: RemoteDataState
}

type Action =
  | {type: 'SET_IS_SERIOUS'; isSerious: boolean}
  | {type: 'SET_BUCKET_NAME'; bucketName: string}
  | {type: 'SET_TIME_RANGE'; timeRange: [number, number]}
  | {type: 'SET_FILTER'; filter: Filter; index: number}
  | {type: 'DELETE_FILTER'; index: number}
  | {type: 'SET_DELETION_STATUS'; deletionStatus: RemoteDataState}

const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case 'SET_IS_SERIOUS':
      return {...state, isSerious: action.isSerious}

    case 'SET_BUCKET_NAME':
      return {...state, bucketName: action.bucketName}

    case 'SET_TIME_RANGE':
      return {...state, timeRange: action.timeRange}

    case 'SET_FILTER':
      if (action.index >= state.filters.length) {
        return {...state, filters: [...state.filters, action.filter]}
      }

      return {
        ...state,
        filters: state.filters.map((filter, i) =>
          i === action.index ? action.filter : filter
        ),
      }

    case 'DELETE_FILTER':
      return {
        ...state,
        filters: state.filters.filter((_, i) => i !== action.index),
      }

    case 'SET_DELETION_STATUS':
      return {...state, deletionStatus: action.deletionStatus}

    default:
      throw new Error('unhandled reducer action in <DeleteDataForm />')
  }
}

const DeleteDataForm: FunctionComponent<Props> = ({
  orgID,
  initialBucketName,
  initialTimeRange,
}) => {
  const recently = Date.parse(moment().format('YYYY-MM-DD HH:00:00'))

  const initialState: State = {
    bucketName: initialBucketName,
    filters: [],
    timeRange: initialTimeRange || [recently - HOUR_MS, recently],
    isSerious: false,
    deletionStatus: RemoteDataState.NotStarted,
  }

  const [state, dispatch] = useReducer(reducer, initialState)

  const canDelete =
    state.isSerious &&
    state.deletionStatus === RemoteDataState.NotStarted &&
    state.filters.every(f => !!f.key && !!f.value)

  const handleDelete = async () => {
    try {
      dispatch({
        type: 'SET_DELETION_STATUS',
        deletionStatus: RemoteDataState.Loading,
      })

      const deleteRequest = {
        query: {
          bucket: state.bucketName,
          org: orgID,
          precision: 'ms',
        },

        body: {
          start: state.timeRange[0],
          stop: state.timeRange[1],
          tags: state.filters,
        },
      }

      await FAKE_API_CALL(deleteRequest)

      dispatch({
        type: 'SET_DELETION_STATUS',
        deletionStatus: RemoteDataState.Done,
      })
    } catch {
      dispatch({
        type: 'SET_DELETION_STATUS',
        deletionStatus: RemoteDataState.Error,
      })
    }
  }

  return (
    <Form className="delete-data-form">
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Four}>
            <Form.Element label="Target Bucket">
              <BucketsDropdown
                bucketName={state.bucketName}
                onSetBucketName={bucketName =>
                  dispatch({type: 'SET_BUCKET_NAME', bucketName})
                }
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight}>
            <Form.Element label="Time Range">
              <TimeRangeDropdown
                timeRange={state.timeRange}
                onSetTimeRange={timeRange =>
                  dispatch({type: 'SET_TIME_RANGE', timeRange})
                }
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <FilterEditor
              filters={state.filters}
              onSetFilter={(filter, index) =>
                dispatch({type: 'SET_FILTER', filter, index})
              }
              onDeleteFilter={index => dispatch({type: 'DELETE_FILTER', index})}
              shouldValidate={state.isSerious}
            />
          </Grid.Column>
        </Grid.Row>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Twelve}>
            <Panel className="delete-data-form--danger-zone">
              <Panel.Header title="Danger Zone!" />
              <Panel.Body className="delete-data-form--confirm">
                <Checkbox
                  label="I understand that this cannot be undone."
                  checked={state.isSerious}
                  onSetChecked={isSerious =>
                    dispatch({type: 'SET_IS_SERIOUS', isSerious})
                  }
                />
                <DeleteButton
                  status={state.deletionStatus}
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

export default DeleteDataForm
