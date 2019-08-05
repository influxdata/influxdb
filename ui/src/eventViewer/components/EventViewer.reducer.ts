// Types
import {Dispatch as ReactDispatch} from 'react'
import {RemoteDataState} from 'src/types'
import {Row, LoadRows, SearchExpr} from 'src/eventViewer/types'

export interface State {
  // All rows currently in the table
  rows: Row[]

  // The offset to use next time we load data
  offset: number

  // The number of rows to load next time we load data
  limit: number

  // The definition of "now" when running queries that are relative to "now"
  now: number | null

  // A expression used to filter results when we load rows
  searchExpr: SearchExpr | null

  // Tracks the loading status of the next rows being loading
  nextRowsStatus: RemoteDataState

  // If the next rows failed to load, this field should contain a
  // human-friendly error message indicating why the rows failed to load
  nextRowsErrorMessage: string | null

  // A function that can be used to cancel any rows currently being loaded
  nextRowsCanceller: null | (() => void)

  // If a user has loaded all the rows that exist
  hasReachedEnd: boolean

  // When set, the table will render with this index in view
  nextScrollIndex: number | null

  // How many pixels into the table we have currently scrolled
  scrollTop: number
}

export type Action =
  | {type: 'NEXT_ROWS_LOADED'; rows: Row[]}
  | {type: 'NEXT_ROWS_FAILED_TO_LOAD'; errorMessage: string}
  | {type: 'NEXT_ROWS_LOADING'; cancel: () => void; now?: number}
  | {type: 'SEARCH_ENTERED'; cancel: () => void; expr: SearchExpr; now: number}
  | {type: 'SEARCH_COMPLETED'; rows: Row[]; limit: number}
  | {type: 'SEARCH_CLEARED'; now: number; cancel: () => void}
  | {type: 'SEARCH_FAILED'; errorMessage: string}
  | {type: 'SCROLLED'; scrollTop: number}
  | {type: 'CONSUMED_NEXT_SCROLL_INDEX'}
  | {type: 'CLICKED_BACK_TO_TOP'}
  | {type: 'LIMIT_CHANGED'; limit: number}
  | {type: 'REFRESHED'; cancel: () => void; now: number}

export type Dispatch = ReactDispatch<Action>

export const INITIAL_STATE: State = {
  rows: [],
  offset: 0,
  limit: 100,
  now: null,
  nextRowsStatus: RemoteDataState.NotStarted,
  nextRowsErrorMessage: null,
  nextRowsCanceller: null,
  hasReachedEnd: false,
  searchExpr: null,
  scrollTop: 0,
  nextScrollIndex: null,
}

export const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case 'NEXT_ROWS_LOADING': {
      return {
        ...state,
        nextRowsStatus: RemoteDataState.Loading,
        nextRowsCanceller: action.cancel,
        now: action.now ? action.now : state.now,
      }
    }

    case 'NEXT_ROWS_FAILED_TO_LOAD': {
      return {
        ...state,
        nextRowsStatus: RemoteDataState.Error,
        nextRowsErrorMessage: action.errorMessage,
      }
    }

    case 'NEXT_ROWS_LOADED': {
      const rows = [...state.rows, ...action.rows]

      return {
        ...state,
        rows,
        nextRowsStatus: RemoteDataState.Done,
        offset: rows.length,
        hasReachedEnd: action.rows.length === 0,
      }
    }

    case 'SEARCH_ENTERED': {
      return {
        ...state,
        rows: [],
        offset: 0,
        now: action.now,
        searchExpr: action.expr,
        nextRowsCanceller: action.cancel,
        nextRowsStatus: RemoteDataState.Loading,
        hasReachedEnd: false,
      }
    }

    case 'SEARCH_COMPLETED': {
      return {
        ...state,
        rows: action.rows,
        nextRowsStatus: RemoteDataState.Done,
        offset: action.rows.length,
        hasReachedEnd: action.rows.length < action.limit,
      }
    }

    case 'SEARCH_FAILED': {
      return {
        ...state,
        nextRowsStatus: RemoteDataState.Error,
        nextRowsErrorMessage: action.errorMessage,
      }
    }

    case 'SEARCH_CLEARED': {
      return {
        ...state,
        rows: [],
        offset: 0,
        now: action.now,
        nextRowsStatus: RemoteDataState.Loading,
        hasReachedEnd: false,
        nextRowsCanceller: action.cancel,
        searchExpr: null,
      }
    }

    case 'SCROLLED': {
      return {...state, scrollTop: action.scrollTop}
    }

    case 'CLICKED_BACK_TO_TOP': {
      return {...state, nextScrollIndex: 0}
    }

    case 'CONSUMED_NEXT_SCROLL_INDEX': {
      return {...state, nextScrollIndex: null}
    }

    case 'LIMIT_CHANGED': {
      return {...state, limit: action.limit}
    }

    case 'REFRESHED': {
      return {
        ...state,
        rows: [],
        offset: 0,
        now: action.now,
        nextRowsStatus: RemoteDataState.Loading,
        hasReachedEnd: false,
        nextRowsCanceller: action.cancel,
      }
    }

    default: {
      const neverAction: never = action

      throw new Error(`unhandled action "${(neverAction as any).type}"`)
    }
  }
}

export const loadNextRows = async (
  state: State,
  dispatch: Dispatch,
  loadRows: LoadRows,
  now?: number
): Promise<void> => {
  if (
    state.nextRowsStatus === RemoteDataState.Loading ||
    state.nextRowsStatus === RemoteDataState.Error ||
    state.hasReachedEnd
  ) {
    return
  }

  try {
    const {promise, cancel} = loadRows({
      offset: state.offset,
      limit: state.limit,
      since: now || state.now,
      filter: state.searchExpr,
    })

    dispatch({type: 'NEXT_ROWS_LOADING', cancel, now})

    const rows = await promise

    dispatch({type: 'NEXT_ROWS_LOADED', rows})
  } catch (error) {
    if (error.name === 'CancellationError') {
      return
    }

    dispatch({type: 'NEXT_ROWS_FAILED_TO_LOAD', errorMessage: error.message})
  }
}

export const search = async (
  state: State,
  dispatch: Dispatch,
  loadRows: LoadRows,
  searchExpr: SearchExpr
): Promise<void> => {
  try {
    if (state.nextRowsCanceller) {
      // Cancel existing pending search, if there is one
      state.nextRowsCanceller()
    }

    const now = Date.now()
    const limit = state.limit

    const {promise, cancel} = loadRows({
      offset: 0,
      limit,
      since: now,
      filter: searchExpr,
    })

    dispatch({type: 'SEARCH_ENTERED', cancel, now, expr: searchExpr})

    const rows = await promise

    dispatch({type: 'SEARCH_COMPLETED', rows, limit})
  } catch (error) {
    if (error.name === 'CancellationError') {
      return
    }

    dispatch({type: 'SEARCH_FAILED', errorMessage: error.message})
  }
}

export const clearSearch = async (
  state: State,
  dispatch: Dispatch,
  loadRows: LoadRows
): Promise<void> => {
  try {
    if (state.nextRowsCanceller) {
      // Cancel existing pending search, if there is one
      state.nextRowsCanceller()
    }

    const now = Date.now()

    const {promise, cancel} = loadRows({
      offset: 0,
      limit: state.limit,
      since: now,
    })

    dispatch({type: 'SEARCH_CLEARED', now, cancel})

    const rows = await promise

    dispatch({type: 'NEXT_ROWS_LOADED', rows})
  } catch (error) {
    if (error.name === 'CancellationError') {
      return
    }

    dispatch({type: 'NEXT_ROWS_FAILED_TO_LOAD', errorMessage: error.message})
  }
}

export const refresh = async (
  state: State,
  dispatch: Dispatch,
  loadRows: LoadRows
): Promise<void> => {
  try {
    if (state.nextRowsCanceller) {
      // Cancel existing pending fetch, if one exists
      state.nextRowsCanceller()
    }

    const now = Date.now()

    const {promise, cancel} = loadRows({
      offset: 0,
      limit: state.limit,
      since: now,
      filter: state.searchExpr,
    })

    dispatch({type: 'REFRESHED', cancel, now})

    const rows = await promise

    dispatch({type: 'NEXT_ROWS_LOADED', rows})
  } catch (error) {
    if (error.name === 'CancellationError') {
      return
    }

    dispatch({type: 'NEXT_ROWS_FAILED_TO_LOAD', errorMessage: error.message})
  }
}

/*
  Given the current state, how many rows should be rendered in the table?
*/
export const getRowCount = (state: State): number => {
  const isInitialLoad =
    state.rows.length === 0 &&
    state.offset === 0 &&
    state.nextRowsStatus === RemoteDataState.Loading

  if (isInitialLoad && state.nextRowsStatus !== RemoteDataState.Error) {
    return state.rows.length + 100
  }

  return state.rows.length + 1
}
