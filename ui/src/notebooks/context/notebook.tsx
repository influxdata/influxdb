import React, {FC, useState, useCallback, RefObject} from 'react'
import {RemoteDataState} from 'src/types'
import {PipeData} from 'src/notebooks'
import {BothResults} from 'src/notebooks/context/query'

export interface PipeMeta {
  title: string
  visible: boolean
  loading: RemoteDataState
  focus: boolean
  panelRef: RefObject<HTMLDivElement>
}

// TODO: this is screaming for normalization. figure out frontend uuids for cells
export interface NotebookContextType {
  id: string
  pipes: PipeData[]
  meta: PipeMeta[] // data only used for the view layer for Notebooks
  results: BothResults[]
  addPipe: (pipe: PipeData) => void
  updatePipe: (idx: number, pipe: PipeData) => void
  updateMeta: (idx: number, pipe: PipeMeta) => void
  updateResult: (idx: number, result: BothResults) => void
  movePipe: (currentIdx: number, newIdx: number) => void
  removePipe: (idx: number) => void
}

export const DEFAULT_CONTEXT: NotebookContextType = {
  id: 'new',
  pipes: [],
  meta: [],
  results: [],
  addPipe: () => {},
  updatePipe: () => {},
  updateMeta: () => {},
  updateResult: () => {},
  movePipe: () => {},
  removePipe: () => {},
}

// NOTE: this just loads some test data for exploring the
// interactions between types. Make sure it's never true
// during the pull review process
const TEST_MODE = false
if (TEST_MODE) {
  const TEST_NOTEBOOK = require('src/notebooks/context/notebook.mock.json')
  DEFAULT_CONTEXT.id = TEST_NOTEBOOK.id
  DEFAULT_CONTEXT.pipes = TEST_NOTEBOOK.pipes
  DEFAULT_CONTEXT.meta = TEST_NOTEBOOK.meta
  DEFAULT_CONTEXT.results = new Array(TEST_NOTEBOOK.pipes.length)
}

export const NotebookContext = React.createContext<NotebookContextType>(
  DEFAULT_CONTEXT
)

let GENERATOR_INDEX = 0

export const NotebookProvider: FC = ({children}) => {
  const [id] = useState(DEFAULT_CONTEXT.id)
  const [pipes, setPipes] = useState(DEFAULT_CONTEXT.pipes)
  const [meta, setMeta] = useState(DEFAULT_CONTEXT.meta)
  const [results, setResults] = useState(DEFAULT_CONTEXT.results)

  const _setPipes = useCallback(setPipes, [id])
  const _setMeta = useCallback(setMeta, [id])
  const _setResults = useCallback(setResults, [id])

  const addPipe = useCallback(
    (pipe: PipeData) => {
      const add = data => {
        return pipes => {
          pipes.push(data)
          return pipes.slice()
        }
      }

      if (
        pipe.type === 'query' &&
        pipes.filter(p => p.type === 'query').length
      ) {
        pipe.queries[0].text =
          '// tip: use the __PREVIOUS_RESULT__ variable to link your queries\n\n' +
          pipe.queries[0].text
      }

      if (pipes.length && pipe.type !== 'query') {
        _setResults(add({...results[results.length - 1]}))
        _setMeta(
          add({
            title: `Cell_${++GENERATOR_INDEX}`,
            visible: true,
            loading: meta[meta.length - 1].loading,
            focus: false,
          })
        )
      } else {
        _setResults(add({}))
        _setMeta(
          add({
            title: `Cell_${++GENERATOR_INDEX}`,
            visible: true,
            loading: RemoteDataState.NotStarted,
            focus: false,
          })
        )
      }
      _setPipes(add(pipe))
    },
    [id, pipes, meta, results]
  )

  const updatePipe = useCallback(
    (idx: number, pipe: PipeData) => {
      _setPipes(pipes => {
        pipes[idx] = {
          ...pipes[idx],
          ...pipe,
        }
        return pipes.slice()
      })
    },
    [id, pipes]
  )

  const updateMeta = useCallback(
    (idx: number, pipe: PipeMeta) => {
      _setMeta(pipes => {
        pipes[idx] = {
          ...pipes[idx],
          ...pipe,
        }
        return pipes.slice()
      })
    },
    [id, meta]
  )

  const updateResult = useCallback(
    (idx: number, results: BothResults) => {
      _setResults(pipes => {
        pipes[idx] = {
          ...results,
        } as BothResults
        return pipes.slice()
      })
    },
    [id, results]
  )

  const movePipe = useCallback(
    (currentIdx: number, newIdx: number) => {
      const move = list => {
        const idx = ((newIdx % list.length) + list.length) % list.length

        if (idx === currentIdx) {
          return list
        }

        const pipe = list.splice(currentIdx, 1)

        list.splice(idx, 0, pipe[0])

        return list.slice()
      }
      _setPipes(move)
      _setMeta(move)
      _setResults(move)
    },
    [id]
  )

  const removePipe = useCallback(
    (idx: number) => {
      const remove = pipes => {
        pipes.splice(idx, 1)
        return pipes.slice()
      }
      _setPipes(remove)
      _setMeta(remove)
      _setResults(remove)
    },
    [id]
  )

  return (
    <NotebookContext.Provider
      value={{
        id,
        pipes,
        meta,
        results,
        updatePipe,
        updateMeta,
        updateResult,
        movePipe,
        addPipe,
        removePipe,
      }}
    >
      {children}
    </NotebookContext.Provider>
  )
}
