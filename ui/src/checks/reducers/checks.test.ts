// Libraries
import {normalize} from 'normalizr'

// Schemas
import {arrayOfChecks, checkSchema} from 'src/schemas/checks'

import checksReducer, {defaultChecksState} from 'src/checks/reducers'

import {setChecks, setCheck, removeCheck} from 'src/checks/actions/creators'
import {
  RemoteDataState,
  DashboardQuery,
  CheckEntities,
  Check,
  GenThresholdCheck,
  GenDeadmanCheck,
} from 'src/types'

const CHECK_QUERY_FIXTURE: DashboardQuery = {
  text: 'this is query',
  editMode: 'advanced',
  builderConfig: null,
  name: 'great q',
}

export const CHECK_FIXTURE_1: GenThresholdCheck = {
  id: '1',
  type: 'threshold',
  name: 'Amoozing check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: CHECK_QUERY_FIXTURE,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'CRIT',
      type: 'lesser',
      value: 10,
    },
  ],
  labels: [],
}

export const CHECK_FIXTURE_2: GenThresholdCheck = {
  id: '2',
  type: 'threshold',
  name: 'Another check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: CHECK_QUERY_FIXTURE,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'INFO',
      type: 'greater',
      value: 10,
    },
  ],
  labels: [],
}

export const CHECK_FIXTURE_3: GenDeadmanCheck = {
  id: '2',
  type: 'deadman',
  name: 'Another check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: CHECK_QUERY_FIXTURE,
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  timeSince: '2m',
  staleTime: '10m',
  reportZero: false,
  level: 'INFO',
  labels: [],
  status: 'active',
}

describe('checksReducer', () => {
  describe('setChecks', () => {
    it('sets list and status properties of state.', () => {
      const initialState = defaultChecksState
      const cid_1 = CHECK_FIXTURE_1.id
      const cid_2 = CHECK_FIXTURE_2.id

      const checks = [CHECK_FIXTURE_1, CHECK_FIXTURE_2]

      const normChecks = normalize<Check, CheckEntities, string[]>(
        checks,
        arrayOfChecks
      )

      const actual = checksReducer(
        initialState,
        setChecks(RemoteDataState.Done, normChecks)
      )

      expect(actual.byID[cid_1]).toEqual({
        ...CHECK_FIXTURE_1,
        activeStatus: 'active',
        status: RemoteDataState.Done,
      })
      expect(actual.byID[cid_2]).toEqual({
        ...CHECK_FIXTURE_2,
        activeStatus: 'active',
        status: RemoteDataState.Done,
      })
      expect(actual.allIDs).toEqual([cid_1, cid_2])
      expect(actual.status).toBe(RemoteDataState.Done)
    })
  })

  describe('setCheck', () => {
    it('adds check to list if it is new', () => {
      const initialState = defaultChecksState
      const id = CHECK_FIXTURE_2.id

      const check = normalize<Check, CheckEntities, string>(
        CHECK_FIXTURE_2,
        checkSchema
      )

      const actual = checksReducer(
        initialState,
        setCheck(id, RemoteDataState.Done, check)
      )

      expect(actual.byID[id]).toEqual({
        ...CHECK_FIXTURE_2,
        status: RemoteDataState.Done,
        activeStatus: 'active',
      })

      expect(actual.allIDs).toEqual([id])
    })
  })

  describe('removeCheck', () => {
    it('removes check from state', () => {
      const initialState = defaultChecksState
      const id = CHECK_FIXTURE_1.id

      initialState.byID[id] = {
        ...(CHECK_FIXTURE_1 as Check),
        status: RemoteDataState.Done,
        activeStatus: 'active',
      }

      initialState.allIDs = [id]

      const actual = checksReducer(
        initialState,
        removeCheck(CHECK_FIXTURE_1.id)
      )

      expect(actual.byID).toEqual({})
      expect(actual.allIDs).toEqual([])
    })
  })
})
