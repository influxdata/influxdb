// Reducer
import {initialState, predicatesReducer} from 'src/shared/reducers/predicates'

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
import {pastHourTimeRange} from 'src/shared/constants/timeRanges'
import {convertTimeRangeToCustom} from 'src/shared/utils/duration'

const filter: Filter = {key: 'mean', equality: '=', value: '100'}

describe('Predicates reducer', () => {
  it('Can set the isSerious property', () => {
    expect(initialState.isSerious).toEqual(false)
    let result = predicatesReducer(initialState, setIsSerious(true))
    expect(result.isSerious).toEqual(true)
    result = predicatesReducer(result, setIsSerious(false))
    expect(result.isSerious).toEqual(false)
  })

  it('Can set the bucketName property', () => {
    const bucketName = 'bucket_list'
    expect(initialState.bucketName).toEqual('')
    const result = predicatesReducer(initialState, setBucketName(bucketName))
    expect(result.bucketName).toEqual(bucketName)
  })

  it('Can set the timeRange property', () => {
    expect(initialState.timeRange).toBeNull()

    const instantiatedPastHour = convertTimeRangeToCustom(pastHourTimeRange)
    const result = predicatesReducer(
      initialState,
      setTimeRange(instantiatedPastHour)
    )
    expect(result.timeRange).toEqual(instantiatedPastHour)
  })

  it('Can set the filter property', () => {
    expect(initialState.filters).toEqual([])
    const result = predicatesReducer(initialState, setFilter(filter, 0))
    expect(result.filters).toEqual([filter])
  })

  it('Can delete a filter that has been set', () => {
    let result = predicatesReducer(initialState, setFilter(filter, 0))
    expect(result.filters).toEqual([filter])
    result = predicatesReducer(initialState, deleteFilter(0))
    expect(result.filters).toEqual([])
  })

  it('Can reset the state after a filter DWP has been successfully submitted', () => {
    const state = initialState
    const intermediateState = predicatesReducer(state, setFilter(filter, 0))
    const result = predicatesReducer(intermediateState, resetPredicateState())
    expect(result).toEqual(state)
  })
})
