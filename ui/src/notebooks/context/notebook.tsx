import React, {FC, useState, useCallback} from 'react'
import {PipeData} from 'src/notebooks'

export interface PipeMeta {
  title: string
  visible: boolean
  focus: boolean
}

export interface NotebookContextType {
  id: string
  pipes: PipeData[]
  meta: PipeMeta[] // data only used for the view layer for Notebooks
  addPipe: (pipe: PipeData) => void
  updatePipe: (idx: number, pipe: PipeData) => void
  updateMeta: (idx: number, pipe: PipeMeta) => void
  movePipe: (currentIdx: number, newIdx: number) => void
  removePipe: (idx: number) => void
}

export const DEFAULT_CONTEXT: NotebookContextType = {
  id: 'new',
  pipes: [],
  meta: [],
  addPipe: () => {},
  updatePipe: () => {},
  updateMeta: () => {},
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
}

export const NotebookContext = React.createContext<NotebookContextType>(
  DEFAULT_CONTEXT
)

let GENERATOR_INDEX = 0

export const NotebookProvider: FC = ({children}) => {
  const [id] = useState(DEFAULT_CONTEXT.id)
  const [pipes, setPipes] = useState(DEFAULT_CONTEXT.pipes)
  const [meta, setMeta] = useState(DEFAULT_CONTEXT.meta)

  const _setPipes = useCallback(setPipes, [id])
  const _setMeta = useCallback(setMeta, [id])

  const addPipe = useCallback(
    (pipe: PipeData) => {
      const add = data => {
        return pipes => {
          pipes.push(data)
          return pipes.slice()
        }
      }
      _setPipes(add(pipe))
      _setMeta(
        add({
          title: `Cell_${++GENERATOR_INDEX}`,
          visible: true,
        })
      )
    },
    [id]
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
    [id]
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
    [id]
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
    },
    [id]
  )

  return (
    <NotebookContext.Provider
      value={{
        id,
        pipes,
        meta,
        updatePipe,
        updateMeta,
        movePipe,
        addPipe,
        removePipe,
      }}
    >
      {children}
    </NotebookContext.Provider>
  )
}
