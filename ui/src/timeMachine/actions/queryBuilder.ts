// APIs
import {normalize} from 'normalizr'
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'
import * as api from 'src/client'
import {get} from 'lodash'
import {fetchDemoDataBuckets} from 'src/cloud/apis/demodata'

// Utils
import {event} from 'src/cloud/utils/reporting'

// Types
import {
  Bucket,
  BucketEntities,
  BuilderAggregateFunctionType,
  GetState,
  RemoteDataState,
  ResourceType,
} from 'src/types'
import {Dispatch} from 'react'
import {BuilderFunctionsType} from '@influxdata/influx'
import {
  Action as AlertBuilderAction,
  setEvery,
} from 'src/alerting/actions/alertBuilder'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'
import {getStatus} from 'src/resources/selectors'
import {getTimeRangeWithTimezone} from 'src/dashboards/selectors'
import {
  getActiveQuery,
  getActiveTimeMachine,
  getWindowPeriodFromTimeRange,
} from 'src/timeMachine/selectors'

//Actions
import {editActiveQueryWithBuilderSync} from 'src/timeMachine/actions'
import {setBuckets} from 'src/buckets/actions/creators'

// Constants
import {LIMIT} from 'src/resources/constants'
import {AGG_WINDOW_AUTO} from 'src/timeMachine/constants/queryBuilder'

// Schemas
import {arrayOfBuckets} from 'src/schemas'

export type Action =
  | ReturnType<typeof setBuilderAggregateFunctionType>
  | ReturnType<typeof setBuilderBucket>
  | ReturnType<typeof setBuilderBuckets>
  | ReturnType<typeof setBuilderBucketsStatus>
  | ReturnType<typeof setBuilderTagKeys>
  | ReturnType<typeof setBuilderTagKeysStatus>
  | ReturnType<typeof setBuilderTagValues>
  | ReturnType<typeof setBuilderTagValuesStatus>
  | ReturnType<typeof setBuilderTagKeySelection>
  | ReturnType<typeof setBuilderTagValuesSelection>
  | ReturnType<typeof addTagSelectorSync>
  | ReturnType<typeof removeTagSelectorSync>
  | ReturnType<typeof setFunctions>
  | ReturnType<typeof setAggregateWindow>
  | ReturnType<typeof setAggregateFillValues>
  | ReturnType<typeof setValuesSearchTerm>
  | ReturnType<typeof setKeysSearchTerm>
  | ReturnType<typeof setBuilderTagsStatus>

export const setBuilderAggregateFunctionType = (
  builderAggregateFunctionType: BuilderAggregateFunctionType,
  index: number
) => ({
  type: 'SET_BUILDER_AGGREGATE_FUNCTION_TYPE' as 'SET_BUILDER_AGGREGATE_FUNCTION_TYPE',
  payload: {builderAggregateFunctionType, index},
})

const setBuilderBucketsStatus = (bucketsStatus: RemoteDataState) => ({
  type: 'SET_BUILDER_BUCKETS_STATUS' as 'SET_BUILDER_BUCKETS_STATUS',
  payload: {bucketsStatus},
})

export const setBuilderBuckets = (buckets: string[]) => ({
  type: 'SET_BUILDER_BUCKETS' as 'SET_BUILDER_BUCKETS',
  payload: {buckets},
})

const setBuilderBucket = (bucket: string, resetSelections: boolean) => ({
  type: 'SET_BUILDER_BUCKET_SELECTION' as 'SET_BUILDER_BUCKET_SELECTION',
  payload: {bucket, resetSelections},
})

export const setBuilderTagsStatus = (status: RemoteDataState) => ({
  type: 'SET_BUILDER_TAGS_STATUS' as 'SET_BUILDER_TAGS_STATUS',
  payload: {status},
})

const setBuilderTagKeys = (index: number, keys: string[]) => ({
  type: 'SET_BUILDER_TAG_KEYS' as 'SET_BUILDER_TAG_KEYS',
  payload: {index, keys},
})

export const setBuilderTagKeysStatus = (
  index: number,
  status: RemoteDataState
) => ({
  type: 'SET_BUILDER_TAG_KEYS_STATUS' as 'SET_BUILDER_TAG_KEYS_STATUS',
  payload: {index, status},
})

const setBuilderTagValues = (index: number, values: string[]) => ({
  type: 'SET_BUILDER_TAG_VALUES' as 'SET_BUILDER_TAG_VALUES',
  payload: {index, values},
})

const setBuilderTagValuesStatus = (index: number, status: RemoteDataState) => ({
  type: 'SET_BUILDER_TAG_VALUES_STATUS' as 'SET_BUILDER_TAG_VALUES_STATUS',
  payload: {index, status},
})

const setBuilderTagKeySelection = (index: number, key: string) => ({
  type: 'SET_BUILDER_TAG_KEY_SELECTION' as 'SET_BUILDER_TAG_KEY_SELECTION',
  payload: {index, key},
})

const setBuilderTagValuesSelection = (index: number, values: string[]) => ({
  type: 'SET_BUILDER_TAG_VALUES_SELECTION' as 'SET_BUILDER_TAG_VALUES_SELECTION',
  payload: {index, values},
})

const addTagSelectorSync = () => ({
  type: 'ADD_TAG_SELECTOR' as 'ADD_TAG_SELECTOR',
})

const removeTagSelectorSync = (index: number) => ({
  type: 'REMOVE_TAG_SELECTOR' as 'REMOVE_TAG_SELECTOR',
  payload: {index},
})

export const setFunctions = (functions: string[]) => ({
  type: 'SELECT_BUILDER_FUNCTION' as 'SELECT_BUILDER_FUNCTION',
  payload: {functions},
})

export const setAggregateWindow = (period: string) => ({
  type: 'SET_AGGREGATE_WINDOW' as 'SET_AGGREGATE_WINDOW',
  payload: {period},
})

export const setAggregateFillValues = (fillValues: boolean) => ({
  type: 'SET_AGGREGATE_FILL_VALUES' as 'SET_AGGREGATE_FILL_VALUES',
  payload: {fillValues},
})

export const setValuesSearchTerm = (index: number, searchTerm: string) => ({
  type: 'SET_BUILDER_VALUES_SEARCH_TERM' as 'SET_BUILDER_VALUES_SEARCH_TERM',
  payload: {index, searchTerm},
})

export const setKeysSearchTerm = (index: number, searchTerm: string) => ({
  type: 'SET_BUILDER_KEYS_SEARCH_TERM' as 'SET_BUILDER_KEYS_SEARCH_TERM',
  payload: {index, searchTerm},
})

export const setWindowPeriodSelectionMode = (mode: 'custom' | 'auto') => (
  dispatch: Dispatch<Action | AlertBuilderAction>,
  getState: GetState
) => {
  if (mode === 'custom') {
    const windowPeriod = getWindowPeriodFromTimeRange(getState())

    dispatch(setAggregateWindow(windowPeriod))
  }
  if (mode === 'auto') {
    dispatch(setAggregateWindow(AGG_WINDOW_AUTO))
  }
}

export const selectAggregateWindow = (period: string) => (
  dispatch: Dispatch<Action | AlertBuilderAction>
) => {
  dispatch(setAggregateWindow(period))
  dispatch(setEvery(period))
}

export const loadBuckets = () => async (
  dispatch: Dispatch<
    Action | ReturnType<typeof selectBucket> | ReturnType<typeof setBuckets>
  >,
  getState: GetState
) => {
  if (
    getStatus(getState(), ResourceType.Buckets) === RemoteDataState.NotStarted
  ) {
    dispatch(setBuckets(RemoteDataState.Loading))
  }
  const startTime = Date.now()
  const orgID = getOrg(getState()).id
  dispatch(setBuilderBucketsStatus(RemoteDataState.Loading))

  try {
    const resp = await api.getBuckets({query: {orgID, limit: LIMIT}})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const demoDataBuckets = await fetchDemoDataBuckets()

    const normalizedBuckets = normalize<Bucket, BucketEntities, string[]>(
      [...resp.data.buckets, ...demoDataBuckets],
      arrayOfBuckets
    )

    dispatch(setBuckets(RemoteDataState.Done, normalizedBuckets))

    const allBuckets = [...resp.data.buckets, ...demoDataBuckets].map(
      b => b.name
    )

    const systemBuckets = allBuckets.filter(b => b.startsWith('_'))
    const userBuckets = allBuckets.filter(b => !b.startsWith('_'))
    const buckets = [...userBuckets, ...systemBuckets]

    const selectedBucket = getActiveQuery(getState()).builderConfig.buckets[0]

    dispatch(setBuilderBuckets(buckets))

    if (selectedBucket && buckets.includes(selectedBucket)) {
      dispatch(selectBucket(selectedBucket))
    } else {
      dispatch(selectBucket(buckets[0], true))
    }
    event(
      'loadBuckets function',
      {
        time: startTime,
      },
      {duration: Date.now() - startTime}
    )
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setBuilderBucketsStatus(RemoteDataState.Error))
  }
}

export const selectBucket = (
  bucket: string,
  resetSelections: boolean = false
) => (dispatch: Dispatch<Action | ReturnType<typeof loadTagSelector>>) => {
  dispatch(setBuilderBucket(bucket, resetSelections))
  dispatch(loadTagSelector(0))
}

export const loadTagSelector = (index: number) => async (
  dispatch: Dispatch<Action | ReturnType<typeof loadTagSelectorValues>>,
  getState: GetState
) => {
  const startTime = Date.now()

  const {buckets, tags} = getActiveQuery(getState()).builderConfig

  if (!tags[index] || !buckets[0]) {
    return
  }

  dispatch(setBuilderTagKeysStatus(index, RemoteDataState.Loading))

  const state = getState()
  const tagsSelections = tags.slice(0, index)
  const queryURL = getState().links.query.self

  const bucket = buckets[0]

  const allBuckets = getAll<Bucket>(getState(), ResourceType.Buckets)
  const foundBucket = allBuckets.find(b => b.name === bucket)

  const orgID = get(foundBucket, 'orgID', getOrg(getState()).id)

  try {
    const timeRange = getTimeRangeWithTimezone(state)

    const searchTerm = getActiveTimeMachine(state).queryBuilder.tags[index]
      .keysSearchTerm

    const keys = await queryBuilderFetcher.findKeys(index, {
      url: queryURL,
      orgID,
      bucket,
      tagsSelections,
      searchTerm,
      timeRange,
    })

    const {key} = tags[index]

    if (!key) {
      let defaultKey: string

      if (index === 0 && keys.includes('_measurement')) {
        defaultKey = '_measurement'
      } else {
        defaultKey = keys[0]
      }

      dispatch(setBuilderTagKeySelection(index, defaultKey))
    } else if (!keys.includes(key)) {
      // Even if the selected key didn't come back in the results, let it be
      // selected anyway
      keys.unshift(key)
    }

    dispatch(setBuilderTagKeys(index, keys))
    dispatch(loadTagSelectorValues(index))
    event(
      'loadTagSelector function',
      {
        time: startTime,
      },
      {duration: Date.now() - startTime}
    )
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setBuilderTagKeysStatus(index, RemoteDataState.Error))
  }
}

const loadTagSelectorValues = (index: number) => async (
  dispatch: Dispatch<Action | ReturnType<typeof loadTagSelector>>,
  getState: GetState
) => {
  const startTime = Date.now()

  const state = getState()
  const {buckets, tags} = getActiveQuery(state).builderConfig
  const tagsSelections = tags.slice(0, index)
  const queryURL = state.links.query.self

  if (!buckets[0]) {
    return
  }

  const bucket = buckets[0]

  const allBuckets = getAll<Bucket>(state, ResourceType.Buckets)
  const foundBucket = allBuckets.find(b => b.name === bucket)
  const orgID = get(foundBucket, 'orgID', getOrg(getState()).id)

  dispatch(setBuilderTagValuesStatus(index, RemoteDataState.Loading))

  try {
    const timeRange = getTimeRangeWithTimezone(state)
    const key = getActiveQuery(getState()).builderConfig.tags[index].key
    const searchTerm = getActiveTimeMachine(getState()).queryBuilder.tags[index]
      .valuesSearchTerm

    const values = await queryBuilderFetcher.findValues(index, {
      url: queryURL,
      orgID,
      bucket,
      tagsSelections,
      key,
      searchTerm,
      timeRange,
    })

    const {values: selectedValues} = tags[index]

    for (const selectedValue of selectedValues) {
      // Even if the selected values didn't come back in the results, let them
      // be selected anyway
      if (!values.includes(selectedValue)) {
        values.unshift(selectedValue)
      }
    }

    dispatch(setBuilderTagValues(index, values))
    dispatch(loadTagSelector(index + 1))
    event(
      'loadTagSelectorValues function',
      {
        time: startTime,
      },
      {duration: Date.now() - startTime}
    )
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setBuilderTagValuesStatus(index, RemoteDataState.Error))
  }
}

export const selectTagValue = (index: number, value: string) => (
  dispatch: Dispatch<Action | ReturnType<typeof addTagSelector>>,
  getState: GetState
) => {
  const state = getState()
  const {
    timeMachines: {activeTimeMachineID},
  } = state
  const tags = getActiveQuery(state).builderConfig.tags
  const currentTag = tags[index]
  const values = currentTag.values

  let newValues: string[]

  if (values.includes(value)) {
    newValues = values.filter(v => v !== value)
  } else if (
    activeTimeMachineID === 'alerting' &&
    currentTag.key === '_field'
  ) {
    newValues = [value]
  } else {
    newValues = [...values, value]
  }

  dispatch(setBuilderTagValuesSelection(index, newValues))

  // don't add a new tag filter if we're grouping
  if (currentTag.aggregateFunctionType === 'group') {
    return
  }

  if (index === tags.length - 1 && newValues.length) {
    dispatch(addTagSelector())
  } else {
    dispatch(loadTagSelector(index + 1))
  }
}

export const multiSelectBuilderFunction = (name: string) => (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(getState())

  const functions = draftQueries[activeQueryIndex].builderConfig.functions

  let newFunctions: BuilderFunctionsType[]

  if (functions.find(f => f.name === name)) {
    if (functions.length == 1) {
      // at least one function must be selected
      return
    }
    newFunctions = functions.filter(f => f.name !== name)
  } else {
    newFunctions = [...functions, {name}]
  }

  dispatch(setFunctions(newFunctions.map(f => f.name)))
}

export const singleSelectBuilderFunction = (name: string) => (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(getState())

  const functions = draftQueries[activeQueryIndex].builderConfig.functions

  let newFunctions: BuilderFunctionsType[]

  if (functions.find(f => f.name === name)) {
    if (functions.length == 1) {
      // at least one function must be selected
      return
    }
    newFunctions = functions.filter(f => f.name !== name)
  } else {
    newFunctions = [{name}]
  }

  dispatch(setFunctions(newFunctions.map(f => f.name)))
}

export const selectTagKey = (index: number, key: string) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(setBuilderTagKeySelection(index, key))
  dispatch(loadTagSelectorValues(index))
}

export const searchTagValues = (index: number) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(loadTagSelectorValues(index))
}

export const searchTagKeys = (index: number) => (
  dispatch: Dispatch<Action>
) => {
  dispatch(loadTagSelector(index))
}

export const addTagSelector = () => (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  dispatch(addTagSelectorSync())

  const newIndex = getActiveQuery(getState()).builderConfig.tags.length - 1

  dispatch(loadTagSelector(newIndex))
}

export const removeTagSelector = (index: number) => (
  dispatch: Dispatch<Action>
) => {
  queryBuilderFetcher.cancelFindValues(index)
  queryBuilderFetcher.cancelFindKeys(index)

  dispatch(removeTagSelectorSync(index))
  dispatch(loadTagSelector(index))
}

export const reloadTagSelectors = () => (dispatch: Dispatch<Action>) => {
  dispatch(setBuilderTagsStatus(RemoteDataState.Loading))
  dispatch(loadTagSelector(0))
}

export const setBuilderBucketIfExists = (bucketName: string) => (
  dispatch: Dispatch<
    Action | ReturnType<typeof editActiveQueryWithBuilderSync>
  >,
  getState: GetState
) => {
  const buckets = getAll<Bucket>(getState(), ResourceType.Buckets)
  if (buckets.find(b => b.name === bucketName)) {
    dispatch(editActiveQueryWithBuilderSync())
    dispatch(setBuilderBucket(bucketName, true))
  }
}
