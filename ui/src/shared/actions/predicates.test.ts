import {mocked} from 'ts-jest/utils'

// Mocks
import {postDelete} from 'src/client'
import {localState} from 'src/mockState'

jest.mock('src/client')
jest.mock('src/timeMachine/apis/queryBuilder')
jest.mock('src/shared/apis/query')

// Types
import {predicateDeleteSucceeded} from 'src/shared/copy/notifications'
import {pastHourTimeRange} from 'src/shared/constants/timeRanges'

// Actions
import {deleteWithPredicate} from 'src/shared/actions/predicates'
import {convertTimeRangeToCustom} from '../utils/duration'

const mockGetState = jest.fn(_ => {
  return {
    ...localState,
    resources: {
      ...localState.resources,
      orgs: {
        byID: {
          '1': '1',
        },
        allIDs: ['1'],
        org: {id: '1', name: 'plerps'},
      },
    },
    predicates: {
      timeRange: convertTimeRangeToCustom(pastHourTimeRange),
      bucketName: 'bucketName',
      filters: [{key: 'k', value: 'v', equality: '='}],
    },
  }
})

describe('Shared.Actions.Predicates', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('deletes then dispatches success messages', async () => {
    const mockDispatch = jest.fn()

    mocked(postDelete).mockImplementation(() => ({status: 204}))
    await deleteWithPredicate()(mockDispatch, mockGetState)

    expect(postDelete).toHaveBeenCalledTimes(1)
    const [
      firstSetDeletionStatusDispatch,
      secondSetDeletionStatusDispatch,
      notifySuccessCall,
      resetPredicateStateCall,
    ] = mockDispatch.mock.calls

    expect(firstSetDeletionStatusDispatch).toEqual([
      {
        type: 'SET_DELETION_STATUS',
        payload: {deletionStatus: 'Loading'},
      },
    ])

    expect(secondSetDeletionStatusDispatch).toEqual([
      {
        type: 'SET_DELETION_STATUS',
        payload: {deletionStatus: 'Done'},
      },
    ])

    expect(notifySuccessCall).toEqual([
      {
        type: 'PUBLISH_NOTIFICATION',
        payload: {
          notification: predicateDeleteSucceeded(),
        },
      },
    ])

    expect(resetPredicateStateCall).toEqual([{type: 'SET_PREDICATE_DEFAULT'}])
  })
})
