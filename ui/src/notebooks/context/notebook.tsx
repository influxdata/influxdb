import React, {FC, useState, useCallback, RefObject} from 'react'
import {RemoteDataState} from 'src/types'
import {PipeData} from 'src/notebooks'
import {FluxResult} from 'src/notebooks'

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
  results: FluxResult[]
  addPipe: (pipe: PipeData, insertAtIndex?: number) => void
  updatePipe: (idx: number, pipe: Partial<PipeData>) => void
  updateMeta: (idx: number, pipe: Partial<PipeMeta>) => void
  updateResult: (idx: number, result: Partial<FluxResult>) => void
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

const getHumanReadableName = (type: string): string => {
  ++GENERATOR_INDEX

  switch (type) {
    case 'data':
      return `Bucket ${GENERATOR_INDEX}`
    case 'visualization':
      return `Visualization ${GENERATOR_INDEX}`
    case 'markdown':
      return `Markdown ${GENERATOR_INDEX}`
    case 'query':
      return `Flux Script ${GENERATOR_INDEX}`
    default:
      return `Cell ${GENERATOR_INDEX}`
  }
}

export const NotebookProvider: FC = ({children}) => {
  const [id] = useState(DEFAULT_CONTEXT.id)
  const [pipes, setPipes] = useState(DEFAULT_CONTEXT.pipes)
  const [meta, setMeta] = useState(DEFAULT_CONTEXT.meta)
  const [results, setResults] = useState(DEFAULT_CONTEXT.results)

  const _setPipes = useCallback(setPipes, [id, setPipes])
  const _setMeta = useCallback(setMeta, [id, setMeta])
  const _setResults = useCallback(setResults, [id, setResults])

  const addPipe = useCallback(
    (pipe: PipeData, insertAtIndex?: number) => {
      let add = data => {
        return pipes => {
          pipes.push(data)
          return pipes.slice()
        }
      }

      if (insertAtIndex !== undefined) {
        add = data => {
          return pipes => {
            pipes.splice(insertAtIndex + 1, 0, data)
            return pipes.slice()
          }
        }
      }

      if (pipes.length && pipe.type !== 'query') {
        _setResults(add({...results[results.length - 1]}))
        _setMeta(
          add({
            title: getHumanReadableName(pipe.type),
            visible: true,
            loading: meta[meta.length - 1].loading,
            focus: false,
          })
        )
      } else {
        _setResults(add({}))
        _setMeta(
          add({
            title: getHumanReadableName(pipe.type),
            visible: true,
            loading: RemoteDataState.NotStarted,
            focus: false,
          })
        )
      }

      if (pipe.type === 'query') {
        if (!pipes.filter(p => p.type === 'query').length) {
          _setPipes(add(pipe))
        } else {
          const _pipe = {
            ...pipe,
            queries: [
              {
                ...pipe.queries[0],
              },
            ],
          }
          _setPipes(add(_pipe))
        }
      } else {
        _setPipes(add(pipe))
      }
    },
    [pipes, meta, results, _setPipes, _setMeta, _setResults]
  )

  const updatePipe = useCallback(
    (idx: number, pipe: Partial<PipeData>) => {
      _setPipes(pipes => {
        pipes[idx] = {
          ...pipes[idx],
          ...pipe,
        }
        return pipes.slice()
      })
    },
    [_setPipes]
  )

  const updateMeta = useCallback(
    (idx: number, pipe: Partial<PipeMeta>) => {
      _setMeta(pipes => {
        pipes[idx] = {
          ...pipes[idx],
          ...pipe,
        }
        return pipes.slice()
      })
    },
    [_setMeta]
  )

  const updateResult = useCallback(
    (idx: number, results: Partial<FluxResult>) => {
      _setResults(pipes => {
        pipes[idx] = {
          ...results,
        } as FluxResult
        return pipes.slice()
      })
    },
    [_setResults]
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
    [_setPipes, _setResults, _setMeta]
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
    [_setPipes, _setMeta, _setResults]
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
