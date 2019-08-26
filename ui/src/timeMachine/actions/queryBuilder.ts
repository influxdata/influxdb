// APIs
import {queryBuilderFetcher} from 'src/timeMachine/apis/QueryBuilderFetcher'

// Utils
import {getActiveQuery, getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Dispatch} from 'redux-thunk'
import {GetState} from 'src/types'
import {RemoteDataState} from 'src/types'
import {BuilderFunctionsType} from '@influxdata/influx'

export type Action =
  | SetBuilderBucketSelectionAction
  | SetBuilderBucketsAction
  | SetBuilderBucketsStatusAction
  | SetBuilderTagKeysAction
  | SetBuilderTagKeysStatusAction
  | SetBuilderTagValuesAction
  | SetBuilderTagValuesStatusAction
  | SetBuilderTagKeySelectionAction
  | SetBuilderTagValuesSelectionAction
  | AddTagSelectorAction
  | RemoveTagSelectorAction
  | SetFunctionsAction
  | SelectAggregateWindowAction
  | SetValuesSearchTermAction
  | SetKeysSearchTermAction
  | SetBuilderTagsStatusAction

interface SetBuilderBucketsStatusAction {
  type: 'SET_BUILDER_BUCKETS_STATUS'
  payload: {bucketsStatus: RemoteDataState}
}

const setBuilderBucketsStatus = (
  bucketsStatus: RemoteDataState
): SetBuilderBucketsStatusAction => ({
  type: 'SET_BUILDER_BUCKETS_STATUS',
  payload: {bucketsStatus},
})

interface SetBuilderBucketsAction {
  type: 'SET_BUILDER_BUCKETS'
  payload: {buckets: string[]}
}

export const setBuilderBuckets = (
  buckets: string[]
): SetBuilderBucketsAction => ({
  type: 'SET_BUILDER_BUCKETS',
  payload: {buckets},
})

interface SetBuilderBucketSelectionAction {
  type: 'SET_BUILDER_BUCKET_SELECTION'
  payload: {bucket: string; resetSelections: boolean}
}

const setBuilderBucket = (
  bucket: string,
  resetSelections: boolean
): SetBuilderBucketSelectionAction => ({
  type: 'SET_BUILDER_BUCKET_SELECTION',
  payload: {bucket, resetSelections},
})

interface SetBuilderTagsStatusAction {
  type: 'SET_BUILDER_TAGS_STATUS'
  payload: {status: RemoteDataState}
}

export const setBuilderTagsStatus = (
  status: RemoteDataState
): SetBuilderTagsStatusAction => ({
  type: 'SET_BUILDER_TAGS_STATUS',
  payload: {status},
})

interface SetBuilderTagKeysAction {
  type: 'SET_BUILDER_TAG_KEYS'
  payload: {index: number; keys: string[]}
}

const setBuilderTagKeys = (
  index: number,
  keys: string[]
): SetBuilderTagKeysAction => ({
  type: 'SET_BUILDER_TAG_KEYS',
  payload: {index, keys},
})

interface SetBuilderTagKeysStatusAction {
  type: 'SET_BUILDER_TAG_KEYS_STATUS'
  payload: {index: number; status: RemoteDataState}
}

const setBuilderTagKeysStatus = (
  index: number,
  status: RemoteDataState
): SetBuilderTagKeysStatusAction => ({
  type: 'SET_BUILDER_TAG_KEYS_STATUS',
  payload: {index, status},
})

interface SetBuilderTagValuesAction {
  type: 'SET_BUILDER_TAG_VALUES'
  payload: {index: number; values: string[]}
}

const setBuilderTagValues = (
  index: number,
  values: string[]
): SetBuilderTagValuesAction => ({
  type: 'SET_BUILDER_TAG_VALUES',
  payload: {index, values},
})

interface SetBuilderTagValuesStatusAction {
  type: 'SET_BUILDER_TAG_VALUES_STATUS'
  payload: {index: number; status: RemoteDataState}
}

const setBuilderTagValuesStatus = (
  index: number,
  status: RemoteDataState
): SetBuilderTagValuesStatusAction => ({
  type: 'SET_BUILDER_TAG_VALUES_STATUS',
  payload: {index, status},
})

interface SetBuilderTagKeySelectionAction {
  type: 'SET_BUILDER_TAG_KEY_SELECTION'
  payload: {index: number; key: string}
}

const setBuilderTagKeySelection = (
  index: number,
  key: string
): SetBuilderTagKeySelectionAction => ({
  type: 'SET_BUILDER_TAG_KEY_SELECTION',
  payload: {index, key},
})

interface SetBuilderTagValuesSelectionAction {
  type: 'SET_BUILDER_TAG_VALUES_SELECTION'
  payload: {index: number; values: string[]}
}

const setBuilderTagValuesSelection = (
  index: number,
  values: string[]
): SetBuilderTagValuesSelectionAction => ({
  type: 'SET_BUILDER_TAG_VALUES_SELECTION',
  payload: {index, values},
})

interface AddTagSelectorAction {
  type: 'ADD_TAG_SELECTOR'
}

const addTagSelectorSync = (): AddTagSelectorAction => ({
  type: 'ADD_TAG_SELECTOR',
})

interface RemoveTagSelectorAction {
  type: 'REMOVE_TAG_SELECTOR'
  payload: {index: number}
}

const removeTagSelectorSync = (index: number): RemoveTagSelectorAction => ({
  type: 'REMOVE_TAG_SELECTOR',
  payload: {index},
})

interface SetFunctionsAction {
  type: 'SELECT_BUILDER_FUNCTION'
  payload: {functions: BuilderFunctionsType[]}
}

export const setFunctions = (
  functions: BuilderFunctionsType[]
): SetFunctionsAction => ({
  type: 'SELECT_BUILDER_FUNCTION',
  payload: {functions},
})

interface SelectAggregateWindowAction {
  type: 'SELECT_AGGREGATE_WINDOW'
  payload: {period: string}
}

export const selectAggregateWindow = (
  period: string
): SelectAggregateWindowAction => ({
  type: 'SELECT_AGGREGATE_WINDOW',
  payload: {period},
})

interface SetValuesSearchTermAction {
  type: 'SET_BUILDER_VALUES_SEARCH_TERM'
  payload: {index: number; searchTerm: string}
}

interface SetKeysSearchTermAction {
  type: 'SET_BUILDER_KEYS_SEARCH_TERM'
  payload: {index: number; searchTerm: string}
}

export const setValuesSearchTerm = (
  index: number,
  searchTerm: string
): SetValuesSearchTermAction => ({
  type: 'SET_BUILDER_VALUES_SEARCH_TERM',
  payload: {index, searchTerm},
})

export const setKeysSearchTerm = (
  index: number,
  searchTerm: string
): SetKeysSearchTermAction => ({
  type: 'SET_BUILDER_KEYS_SEARCH_TERM',
  payload: {index, searchTerm},
})

export const loadBuckets = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const queryURL = getState().links.query.self
  const orgID = getState().orgs.org.id

  dispatch(setBuilderBucketsStatus(RemoteDataState.Loading))

  try {
    const buckets = await queryBuilderFetcher.findBuckets({
      url: queryURL,
      orgID,
    })

    const selectedBucket = getActiveQuery(getState()).builderConfig.buckets[0]

    dispatch(setBuilderBuckets(buckets))

    if (selectedBucket && buckets.includes(selectedBucket)) {
      dispatch(selectBucket(selectedBucket))
    } else {
      dispatch(selectBucket(buckets[0], true))
    }
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
) => async (dispatch: Dispatch<Action>) => {
  dispatch(setBuilderBucket(bucket, resetSelections))
  dispatch(loadTagSelector(0))
}

export const loadTagSelector = (index: number) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const {buckets, tags} = getActiveQuery(getState()).builderConfig

  if (!tags[index] || !buckets[0]) {
    return
  }

  const tagsSelections = tags.slice(0, index)
  const queryURL = getState().links.query.self
  const orgID = getState().orgs.org.id

  dispatch(setBuilderTagKeysStatus(index, RemoteDataState.Loading))

  try {
    const timeRange = getActiveTimeMachine(getState()).timeRange
    const searchTerm = getActiveTimeMachine(getState()).queryBuilder.tags[index]
      .keysSearchTerm

    const keys = await queryBuilderFetcher.findKeys(index, {
      url: queryURL,
      orgID,
      bucket: buckets[0],
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
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setBuilderTagKeysStatus(index, RemoteDataState.Error))
  }
}

const loadTagSelectorValues = (index: number) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const {buckets, tags} = getActiveQuery(state).builderConfig
  const tagsSelections = tags.slice(0, index)
  const queryURL = state.links.query.self
  const orgID = getState().orgs.org.id

  dispatch(setBuilderTagValuesStatus(index, RemoteDataState.Loading))

  try {
    const timeRange = getActiveTimeMachine(getState()).timeRange
    const key = getActiveQuery(getState()).builderConfig.tags[index].key
    const searchTerm = getActiveTimeMachine(getState()).queryBuilder.tags[index]
      .valuesSearchTerm

    const values = await queryBuilderFetcher.findValues(index, {
      url: queryURL,
      orgID,
      bucket: buckets[0],
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
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setBuilderTagValuesStatus(index, RemoteDataState.Error))
  }
}

export const selectTagValue = (index: number, value: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const {
    timeMachines: {activeTimeMachineID},
  } = state
  const tags = getActiveQuery(state).builderConfig.tags
  const values = tags[index].values

  let newValues: string[]

  if (values.includes(value)) {
    newValues = values.filter(v => v !== value)
  } else if (
    activeTimeMachineID === 'alerting' &&
    tags[index].key === '_field'
  ) {
    newValues = [value]
  } else {
    newValues = [...values, value]
  }

  dispatch(setBuilderTagValuesSelection(index, newValues))

  if (index === tags.length - 1 && newValues.length) {
    dispatch(addTagSelector())
  } else {
    dispatch(loadTagSelector(index + 1))
  }
}

export const selectBuilderFunction = (name: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const {
    timeMachines: {activeTimeMachineID},
  } = state
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)

  const functions = draftQueries[activeQueryIndex].builderConfig.functions

  let newFunctions: BuilderFunctionsType[]

  if (functions.find(f => f.name === name)) {
    newFunctions = functions.filter(f => f.name !== name)
  } else if (activeTimeMachineID === 'alerting') {
    newFunctions = [{name}]
  } else {
    newFunctions = [...functions, {name}]
  }
  dispatch(setFunctions(newFunctions))
}

export const selectTagKey = (index: number, key: string) => async (
  dispatch: Dispatch<Action>
) => {
  dispatch(setBuilderTagKeySelection(index, key))
  dispatch(loadTagSelectorValues(index))
}

export const searchTagValues = (index: number) => async (
  dispatch: Dispatch<Action>
) => {
  dispatch(loadTagSelectorValues(index))
}

export const searchTagKeys = (index: number) => async (
  dispatch: Dispatch<Action>
) => {
  dispatch(loadTagSelector(index))
}

export const addTagSelector = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  dispatch(addTagSelectorSync())

  const newIndex = getActiveQuery(getState()).builderConfig.tags.length - 1

  dispatch(loadTagSelector(newIndex))
}

export const removeTagSelector = (index: number) => async (
  dispatch: Dispatch<Action>
) => {
  queryBuilderFetcher.cancelFindValues(index)
  queryBuilderFetcher.cancelFindKeys(index)

  dispatch(removeTagSelectorSync(index))
  dispatch(loadTagSelector(index))
}

export const reloadTagSelectors = () => async (dispatch: Dispatch<Action>) => {
  dispatch(setBuilderTagsStatus(RemoteDataState.Loading))
  dispatch(loadTagSelector(0))
}
