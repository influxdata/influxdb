// Types
import {PredicatesState, RemoteDataState} from 'src/types'
import {TestState} from 'src/shared/selectors/canDelete.test'

export const setCanDelete = (state: PredicatesState | TestState): boolean =>
  state.isSerious &&
  state.deletionStatus === RemoteDataState.NotStarted &&
  state.filters.every(f => !!f.key && !!f.value && !!f.equality)
