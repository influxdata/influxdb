// Utils
import {parseSearchInput} from 'src/eventViewer/utils/search'

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

  // The search text a user has typed into the search bar
  searchInput: string

  // A parsed representation of the whichever search input is currently being
  // used to filter results (not necessarily derived from the current text
  // input, which may not be valid / parseable)
  searchExpr: SearchExpr | null

  // A timeout ID used to debounce performing the search on user input
  searchTimeoutID: any

  // Tracks the loading status of the next rows being loading
  nextRowsStatus: RemoteDataState

  // If the next rows failed to load, this field should contain a
  // human-friendly error message indicating why the rows failed to load
  nextRowsErrorMessage: string | null

  // A function that can be used to cancel any rows currently being loaded
  nextRowsCanceller: null | (() => void)

  // If a user has loaded all the rows that exist
  hasReachedEnd: boolean

  // The current vertical scroll offset, i.e. how many pixels into the table we
  // have currently scrolled
  scrollTop: number

  // When set, the table will render once with this exact vertical scroll
  // offset, then dispatch an action to set this property to null
  nextScrollTop: number | null
}

export type Action =
  | {type: 'NEXT_ROWS_LOADED'; rows: Row[]}
  | {type: 'NEXT_ROWS_FAILED_TO_LOAD'; errorMessage: string}
  | {type: 'NEXT_ROWS_LOADING'; cancel: () => void; now?: number}
  | {type: 'SEARCH_TYPED'; searchInput: string}
  | {type: 'SEARCH_SCHEDULED'; searchTimeoutID: any}
  | {type: 'SEARCH_STARTED'; cancel: () => void; expr: SearchExpr; now: number}
  | {type: 'SEARCH_COMPLETED'; rows: Row[]; limit: number}
  | {type: 'SEARCH_FAILED'; errorMessage: string}
  | {type: 'SEARCH_CLEARED'; now: number; cancel: () => void}
  | {type: 'SCROLLED'; scrollTop: number}
  | {type: 'CONSUMED_NEXT_SCROLL_INDEX'}
  | {type: 'CLICKED_BACK_TO_TOP'}
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
  searchInput: '',
  searchExpr: null,
  searchTimeoutID: null,
  scrollTop: 0,
  nextScrollTop: null,
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

    case 'SEARCH_TYPED': {
      return {...state, searchInput: action.searchInput}
    }

    case 'SEARCH_SCHEDULED': {
      return {...state, searchTimeoutID: action.searchTimeoutID}
    }

    case 'SEARCH_STARTED': {
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
        nextScrollTop: 0,
        hasReachedEnd: false,
        nextRowsCanceller: action.cancel,
        searchExpr: null,
      }
    }

    case 'SCROLLED': {
      return {...state, scrollTop: action.scrollTop}
    }

    case 'CLICKED_BACK_TO_TOP': {
      return {...state, nextScrollTop: 0}
    }

    case 'CONSUMED_NEXT_SCROLL_INDEX': {
      return {...state, nextScrollTop: null}
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
        nextScrollTop: 0,
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
      until: now || state.now,
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

export const search = (
  state: State,
  dispatch: Dispatch,
  loadRows: LoadRows,
  searchInput: string,
  searchImmediately = false
) => {
  clearTimeout(state.searchTimeoutID)
  dispatch({type: 'SEARCH_TYPED', searchInput})

  let searchExpr: SearchExpr | null = null
  let parsingFailed = false

  try {
    searchExpr = parseSearchInput(searchInput)
  } catch {
    parsingFailed = true
  }

  if (parsingFailed || (searchExpr === null && state.searchExpr === null)) {
    return
  } else if (searchExpr === null) {
    clearSearch(state, dispatch, loadRows)
  } else if (searchImmediately) {
    performSearch(state, dispatch, loadRows, searchExpr)
  } else {
    const searchTimeoutID = setTimeout(() => {
      performSearch(state, dispatch, loadRows, searchExpr)
    }, 500)

    dispatch({type: 'SEARCH_SCHEDULED', searchTimeoutID})
  }
}

const performSearch = async (
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
      until: now,
      filter: searchExpr,
    })

    dispatch({type: 'SEARCH_STARTED', cancel, now, expr: searchExpr})

    const rows = await promise

    dispatch({type: 'SEARCH_COMPLETED', rows, limit})
  } catch (error) {
    if (error.name === 'CancellationError') {
      return
    }

    dispatch({type: 'SEARCH_FAILED', errorMessage: error.message})
  }
}

const clearSearch = async (
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
      until: now,
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
      until: now,
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
