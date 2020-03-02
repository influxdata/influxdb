import checksReducer, {defaultChecksState} from 'src/alerting/reducers/checks'
import {setAllChecks, setCheck, removeCheck} from 'src/alerting/actions/checks'
import {
  RemoteDataState,
  DashboardQuery,
  ThresholdCheck,
  DeadmanCheck,
} from 'src/types'

const CHECK_QUERY_FIXTURE: DashboardQuery = {
  text: 'this is query',
  editMode: 'advanced',
  builderConfig: null,
  name: 'great q',
}

export const CHECK_FIXTURE_1: ThresholdCheck = {
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
}

export const CHECK_FIXTURE_2: ThresholdCheck = {
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
}

export const CHECK_FIXTURE_3: DeadmanCheck = {
  id: '2',
  type: 'deadman',
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
  timeSince: '2m',
  staleTime: '10m',
  reportZero: false,
  level: 'INFO',
}

describe('checksReducer', () => {
  describe('setAllChecks', () => {
    it('sets list and status properties of state.', () => {
      const initialState = defaultChecksState

      const actual = checksReducer(
        initialState,
        setAllChecks(RemoteDataState.Done, [CHECK_FIXTURE_1, CHECK_FIXTURE_2])
      )

      const expected = {
        ...defaultChecksState,
        list: [CHECK_FIXTURE_1, CHECK_FIXTURE_2],
        status: RemoteDataState.Done,
      }

      expect(actual).toEqual(expected)
    })
  })
  describe('setCheck', () => {
    it('adds check to list if it is new', () => {
      const initialState = defaultChecksState

      const actual = checksReducer(initialState, setCheck(CHECK_FIXTURE_2))

      const expected = {
        ...defaultChecksState,
        list: [CHECK_FIXTURE_2],
      }

      expect(actual).toEqual(expected)
    })
    it('updates check in list if it exists', () => {
      const initialState = defaultChecksState
      initialState.list = [CHECK_FIXTURE_1]
      const actual = checksReducer(
        initialState,
        setCheck({...CHECK_FIXTURE_1, name: CHECK_FIXTURE_2.name})
      )

      const expected = {
        ...defaultChecksState,
        list: [{...CHECK_FIXTURE_1, name: CHECK_FIXTURE_2.name}],
      }

      expect(actual).toEqual(expected)
    })
  })
  describe('removeCheck', () => {
    it('removes check from list', () => {
      const initialState = defaultChecksState
      initialState.list = [CHECK_FIXTURE_1]
      const actual = checksReducer(
        initialState,
        removeCheck(CHECK_FIXTURE_1.id)
      )

      const expected = {
        ...defaultChecksState,
        list: [],
      }

      expect(actual).toEqual(expected)
    })
  })
})
