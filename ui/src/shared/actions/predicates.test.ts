import {createStore} from 'redux'
import {mocked} from 'ts-jest/utils'

// Mocks
import {postDelete} from 'src/client'
jest.mock('src/client')

// import {extractBoxedCol} from 'src/timeMachine/apis/queryBuilder'
// jest.mock('src/timeMachine/apis/queryBuilder')

// Types

// Reducers
import {initialState, predicatesReducer} from 'src/shared/reducers/predicates'

// Actions
import {deleteWithPredicate} from 'src/shared/actions/predicates'

describe('Shared.Actions.Predicates.setBucketAndKeys', () => {
  let store

  afterEach(() => {
    jest.clearAllMocks()
    store = null
  })

  it('deletes then dispatches success messages', async () => {
    store = createStore(predicatesReducer, initialState)

    const mockDispatch = jest.fn()
    const params = {}

    mocked(postDelete).mockImplementation(() => {
      return {status: 204}
    })
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
      {type: 'PUBLISH_NOTIFICATION', payload: {notification: []}},
    ])

    /*first [
      { type: 'SET_DELETION_STATUS', payload: { deletionStatus: 'Done' } }
    ]

  console.log src/shared/actions/predicates.test.ts:41
    second

  console.log src/shared/actions/predicates.test.ts:42
    third [ { type: 'SET_PREDICATE_DEFAULT' } ]
*/

    // expect(mockDispatch).toHaveBeenCalled()
    // expect(mockDispatch).toHaveBeenNthCalledWith(1, 'setDeletionStatus(RemoteDataState.Done)')

    // expect(mockDispatch).toHaveBeenNthCalledWith(2, )
    // expect(mockDispatch).toHaveBeenNthCalledWith(2, )

    // mocked(getViewFromState).mockImplementation(() => undefined)
    // mocked(getView).mockImplementation(() => Promise.resolve(memoryUsageView))

    // await getViewForTimeMachine(dashboardID, viewID, timeMachineId)(
    //   mockedDispatch,
    //   store.getState
    // )

    // expect(mocked(getView)).toHaveBeenCalledTimes(1)
    // expect(mockedDispatch).toHaveBeenCalledTimes(3)

    // const [
    //   setViewDispatchArguments,
    //   setActiveTimeMachineDispatchArguments,
    // ] = mockedDispatch.mock.calls
    // expect(setViewDispatchArguments[0]).toEqual({
    //   type: 'SET_VIEW',
    //   payload: {id: viewID, view: null, status: RemoteDataState.Loading},
    // })
    // expect(setActiveTimeMachineDispatchArguments[0]).toEqual({
    //   type: 'SET_ACTIVE_TIME_MACHINE',
    //   payload: {
    //     activeTimeMachineID: timeMachineId,
    //     initialState: {view: memoryUsageView},
    //   },
    // })
  })
})
