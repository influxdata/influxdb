// Reducer
import {
  HOUR_MS,
  initialState,
  predicatesReducer,
  recently,
} from 'src/shared/reducers/predicates'

// Types
import {Filter} from 'src/types'

// Actions
import {
  deleteFilter,
  resetPredicateState,
  setBucketName,
  setFilter,
  setIsSerious,
  setTimeRange,
} from 'src/shared/actions/predicates'

describe('Predicates reducer test', () => {
  it('should set the isSerious property', () => {
    expect(initialState.isSerious).toEqual(false)
    let result = predicatesReducer(initialState, setIsSerious(true))
    expect(result.isSerious).toEqual(true)
    result = predicatesReducer(initialState, setIsSerious(false))
    expect(result.isSerious).toEqual(false)
  })
  it('should set the bucketName property', () => {
    const bucketName = 'bucket_list'
    expect(initialState.bucketName).toEqual('')
    const result = predicatesReducer(initialState, setBucketName(bucketName))
    expect(result.bucketName).toEqual(bucketName)
  })
  it('should set the timeRange property', () => {
    expect(initialState.timeRange).toEqual([recently - HOUR_MS, recently])
    const result = predicatesReducer(initialState, setTimeRange([1000, 2000]))
    expect(result.timeRange).toEqual([1000, 2000])
  })
  it('should set the filter property', () => {
    const filter: Filter = {key: 'mean', equality: '=', value: '100'}
    expect(initialState.filters).toEqual([])
    const result = predicatesReducer(initialState, setFilter(filter, 0))
    expect(result.filters).toEqual([filter])
  })
  it('should delete a filter that has been set', () => {
    const filter: Filter = {key: 'mean', equality: '=', value: '100'}
    let result = predicatesReducer(initialState, setFilter(filter, 0))
    expect(result.filters).toEqual([filter])
    result = predicatesReducer(initialState, deleteFilter(0))
    expect(initialState.filters).toEqual([])
  })
  it('should reset the state after a filter DWP has been successfully submitted', () => {
    const state = Object.assign({}, initialState)
    const filter: Filter = {key: 'mean', equality: '=', value: '100'}
    initialState.isSerious = predicatesReducer(
      initialState,
      setIsSerious(true)
    ).isSerious
    initialState.filters = predicatesReducer(
      initialState,
      setFilter(filter, 0)
    ).filters
    const result = predicatesReducer(initialState, resetPredicateState())
    expect(result).toEqual(state)
  })
})
