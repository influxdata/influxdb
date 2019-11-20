import {mocked} from 'ts-jest/utils'

// Mocks
import {postDelete} from 'src/client'
jest.mock('src/client')
jest.mock('src/timeMachine/apis/queryBuilder')
jest.mock('src/shared/apis/query')

// Types

// Actions
import {
  deleteWithPredicate,
  setBucketAndKeys,
  setValuesByKey,
} from 'src/shared/actions/predicates'

describe('Shared.Actions.Predicates', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('deletes then dispatches success messages', async () => {
    const mockDispatch = jest.fn()
    const params = {}

    mocked(postDelete).mockImplementation(() => ({status: 204}))
    await deleteWithPredicate(params)(mockDispatch)

    expect(postDelete).toHaveBeenCalledTimes(1)
    const [
      setDeletionStatusDispatch,
      notifySuccessCall,
      resetPredicateStateCall,
    ] = mockDispatch.mock.calls

    expect(setDeletionStatusDispatch).toEqual([
      {
        type: 'SET_DELETION_STATUS',
        payload: {deletionStatus: 'Done'},
      },
    ])

    expect(notifySuccessCall).toEqual([
      {
        type: 'PUBLISH_NOTIFICATION',
        payload: {
          notification: {
            duration: 5000,
            icon: 'checkmark',
            message: 'Successfully deleted data with predicate!',
            style: 'success',
          },
        },
      },
    ])

    expect(resetPredicateStateCall).toEqual([{type: 'SET_PREDICATE_DEFAULT'}])
  })

  it('sets the keys based on the bucket name', async () => {
    const mockDispatch = jest.fn()
    const orgID = '1'
    const bucketName = 'Foxygen'

    await setBucketAndKeys(orgID, bucketName)(mockDispatch)

    const [setBucketNameDispatch, setKeysDispatch] = mockDispatch.mock.calls

    expect(setBucketNameDispatch).toEqual([
      {type: 'SET_BUCKET_NAME', payload: {bucketName: 'Foxygen'}},
    ])

    expect(setKeysDispatch).toEqual([
      {
        type: 'SET_KEYS_BY_BUCKET',
        payload: {
          keys: ['Talking Heads', 'This must be the place'],
        },
      },
    ])
  })

  it('sets the values based on the bucket and key name', async () => {
    const mockDispatch = jest.fn()
    const orgID = '1'
    const bucketName = 'Simon & Garfunkel'
    const keyName = 'America'

    await setValuesByKey(orgID, bucketName, keyName)(mockDispatch)

    const [setValuesDispatch] = mockDispatch.mock.calls

    expect(setValuesDispatch).toEqual([
      {
        type: 'SET_VALUES_BY_KEY',
        payload: {
          values: ['Talking Heads', 'This must be the place'],
        },
      },
    ])
  })
})
