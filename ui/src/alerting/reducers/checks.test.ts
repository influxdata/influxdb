import checksReducer, {defaultChecksState} from 'src/alerting/reducers/checks'
import {setAllChecks, setCheck, removeCheck} from 'src/alerting/actions/checks'
import {RemoteDataState} from 'src/types'
import {CHECK_FIXTURE_1, CHECK_FIXTURE_2} from 'src/alerting/constants'

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
