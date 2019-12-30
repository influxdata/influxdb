import {RemoteDataState} from 'src/types'
import {setCanDelete} from 'src/shared/selectors/canDelete'

export interface TestState {
  isSerious: boolean
  deletionStatus: RemoteDataState
  filters: any[]
}

describe('canDelete', () => {
  it('should return false when isSerious isFalse', () => {
    const state: TestState = {
      isSerious: false,
      deletionStatus: RemoteDataState.NotStarted,
      filters: [],
    }
    expect(setCanDelete(state)).toEqual(false)
  })
  it('should return false when deletionStatus !== RemoteDataState.NotStarted', () => {
    const state: TestState = {
      isSerious: true,
      deletionStatus: RemoteDataState.Loading,
      filters: [],
    }
    expect(setCanDelete(state)).toEqual(false)
    state.deletionStatus = RemoteDataState.Error
    expect(setCanDelete(state)).toEqual(false)
    state.deletionStatus = RemoteDataState.Done
    expect(setCanDelete(state)).toEqual(false)
  })
  it('should return false when the filter properties are empty', () => {
    const state: TestState = {
      isSerious: true,
      deletionStatus: RemoteDataState.NotStarted,
      filters: [],
    }
    const filter = {
      equality: '=',
    }
    state.filters = [filter]
    expect(setCanDelete(state)).toEqual(false)
    filter['key'] = 'mean'
    state.filters[0] = [filter]
    expect(setCanDelete(state)).toEqual(false)
    delete filter['key']
    filter['value'] = 100
    state.filters[0] = [filter]
    expect(setCanDelete(state)).toEqual(false)
  })
  it('should return true when if filters are empty', () => {
    const state: TestState = {
      isSerious: true,
      deletionStatus: RemoteDataState.NotStarted,
      filters: [],
    }
    expect(setCanDelete(state)).toEqual(true)
  })
  it('should return true when if all props evaluate true', () => {
    const state: TestState = {
      isSerious: true,
      deletionStatus: RemoteDataState.NotStarted,
      filters: [
        {
          key: 'mean',
          equality: '=',
          value: 100,
        },
      ],
    }
    expect(setCanDelete(state)).toEqual(true)
  })
})
