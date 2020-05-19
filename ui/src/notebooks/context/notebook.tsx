import React, {FC, useState} from 'react'

// TODO make this polymorphic to mimic the self registration
// of pipe stages
export type Pipe = any

export interface PipeMeta {
  title: string
  visible: boolean
}

export interface NotebookContextType {
  id: string
  pipes: Pipe[]
  meta: PipeMeta[] // data only used for the view layer for Notebooks
  addPipe: (pipe: Pipe) => void
  updatePipe: (idx: number, pipe: Pipe) => void
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

  function addPipe(pipe: Pipe) {
    const add = data => {
      return pipes => {
        pipes.push(data)
        return pipes.slice()
      }
    }
    setPipes(add(pipe))
    setMeta(
      add({
        title: `Notebook_${++GENERATOR_INDEX}`,
        visible: true,
      })
    )
  }

  function updatePipe(idx: number, pipe: Pipe) {
    setPipes(pipes => {
      pipes[idx] = {
        ...pipes[idx],
        ...pipe,
      }
      return pipes.slice()
    })
  }

  function updateMeta(idx: number, pipe: PipeMeta) {
    setMeta(pipes => {
      pipes[idx] = {
        ...pipes[idx],
        ...pipe,
      }
      return pipes.slice()
    })
  }

  function movePipe(currentIdx: number, newIdx: number) {
    const move = list => {
      const idx = ((newIdx % list.length) + list.length) % list.length

      if (idx === currentIdx) {
        return list
      }

      const pipe = list.splice(currentIdx, 1)

      list.splice(idx, 0, pipe[0])

      return list.slice()
    }
    setPipes(move)
    setMeta(move)
  }

  function removePipe(idx: number) {
    const remove = pipes => {
      pipes.splice(idx, 1)
      return pipes.slice()
    }
    setPipes(remove)
    setMeta(remove)
  }

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
