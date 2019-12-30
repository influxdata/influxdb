import {AppState} from './stores'
import {Action} from 'redux'
import {ThunkAction} from 'redux-thunk'

export type AppThunk<ReturnType = void> = ThunkAction<
  ReturnType,
  AppState,
  null,
  Action<string>
>
