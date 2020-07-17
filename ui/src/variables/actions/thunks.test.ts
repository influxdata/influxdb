/*
 * NOTE:
 *
 * This tests focuses on the hydrateVars thunk, and not the hydrateVariables function
 * As such, we are NOT testing the number of fetch requests made for each variable
 * We ARE testing the number of variables that are ultimately being set based on the
 * query that we are passing in and the available view.
 *
 * The reason for this is simply due to the fact that the hydrateVars thunk is focused
 * on dispatching the setVariables event for each variable that will eventually be fetched
 *
 * Testing the number of fetch requests being made should be tested in the hydrateVariables
 * or hydrateVarHelper since that is more directly related to querying based on the number of variables
 * being injested by the function
 */

// Libraries
import fetchMock from 'jest-fetch-mock'

// Utils
import {hydrateVariables} from 'src/variables/actions/thunks'
import {getMockAppState} from 'src/mockAppState'

const bucketVariableAction = {
  id: '054b7476389f1000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

const deploymentVariableAction = {
  id: '05e6e4df2287b000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

const buildVariableAction = {
  id: '05e6e4fb0887b000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

const brokerHostVariableAction = {
  id: '05ba3253105a5000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

const valuesVariableAction = {
  id: '05aeb0ad75aca000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

const baseQueryVariableAction = {
  id: '05782ef09ddb8000',
  schema: undefined,
  status: 'Loading',
  type: 'SET_VARIABLE',
}

describe('hydrateVariables', () => {
  it('should call hydrateVars once per query variable', async () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() => getMockAppState())

    fetchMock.mockResponse(() => {
      return new Promise(resolve => {
        resolve('')
      })
    })

    hydrateVariables()(dispatch, getState).then(() => {
      expect(dispatch).toHaveBeenCalledWith(bucketVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(baseQueryVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(brokerHostVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(valuesVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(deploymentVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(buildVariableAction)
      expect(dispatch).toHaveBeenCalledTimes(1)
    })
  })
  it('should not call hydrateVars when the query has no variable reference', async () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() =>
      getMockAppState(
        `from(bucket: "Homeward Bound")
        |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
        |> filter(fn: (r) => r["_measurement"] == "cpu")
        |> filter(fn: (r) => r["_field"] == "usage_user")`
      )
    )

    fetchMock.mockResponse(() => {
      return new Promise(resolve => {
        resolve('')
      })
    })

    hydrateVariables()(dispatch, getState).then(() => {
      expect(dispatch).not.toHaveBeenCalledWith(bucketVariableAction)
      expect(dispatch).toHaveBeenCalledTimes(0)
    })
  })
  it('should call hydrateVars twice when there are query variables', async () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() =>
      getMockAppState(
        `from(bucket: v.bucket)
        |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
        |> filter(fn: (r) => r["cpu"] == v.deployment)
        |> filter(fn: (r) => r["_measurement"] == "cpu")
        |> filter(fn: (r) => r["_field"] == "usage_user")`
      )
    )

    fetchMock.mockResponse(() => {
      return new Promise(resolve => {
        resolve('')
      })
    })

    hydrateVariables()(dispatch, getState).then(() => {
      // bucket variable
      expect(dispatch).toHaveBeenCalledWith(bucketVariableAction)
      expect(dispatch).toHaveBeenCalledWith(deploymentVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(baseQueryVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(brokerHostVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(valuesVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(buildVariableAction)
      expect(dispatch).toHaveBeenCalledTimes(2)
    })
  })
  it('should call hydrateVars on all the query variables and their nested variables', async () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() =>
      getMockAppState(
        `from(bucket: v.bucket)
        |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
        |> filter(fn: (r) => r["cpu"] == v.build)
        |> filter(fn: (r) => r["_measurement"] == "cpu")
        |> filter(fn: (r) => r["_field"] == "usage_user")`
      )
    )

    fetchMock.mockResponse(() => {
      return new Promise(resolve => {
        resolve('')
      })
    })

    hydrateVariables()(dispatch, getState).then(() => {
      expect(dispatch).toHaveBeenCalledWith(bucketVariableAction)
      expect(dispatch).toHaveBeenCalledWith(buildVariableAction)
      expect(dispatch).toHaveBeenCalledWith(deploymentVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(baseQueryVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(brokerHostVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(valuesVariableAction)
      expect(dispatch).toHaveBeenCalledTimes(3)
    })
  })
  it('should call all the nested query variables to the one query variable', async () => {
    const dispatch = jest.fn()
    const getState = jest.fn(() =>
      getMockAppState(
        `from(bucket: "Homeward Bound")
        |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
        |> filter(fn: (r) => r["cpu"] == v.build)
        |> filter(fn: (r) => r["_measurement"] == "cpu")
        |> filter(fn: (r) => r["_field"] == "usage_user")`
      )
    )

    fetchMock.mockResponse(() => {
      return new Promise(resolve => {
        resolve('')
      })
    })

    hydrateVariables()(dispatch, getState).then(() => {
      expect(dispatch).toHaveBeenCalledWith(bucketVariableAction)
      expect(dispatch).toHaveBeenCalledWith(buildVariableAction)
      expect(dispatch).toHaveBeenCalledWith(deploymentVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(baseQueryVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(brokerHostVariableAction)
      expect(dispatch).not.toHaveBeenCalledWith(valuesVariableAction)
      expect(dispatch).toHaveBeenCalledTimes(3)
    })
  })
})
