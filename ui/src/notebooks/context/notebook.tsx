import React, {FC, useState} from 'react'

// TODO make this polymorphic to mimic the self registration
// of pipe stages
export type Pipe = any

export interface NotebookContextType {
  id: string
  pipes: Pipe[]
  addPipe: (pipe: Pipe) => void
  updatePipe: (idx: number, pipe: Pipe) => void
  removePipe: (idx: number) => void
}

export const DEFAULT_CONTEXT = {
  id: 'new',
  pipes: [],
  addPipe: () => {},
  updatePipe: () => {},
  removePipe: () => {},
}

// NOTE: this just loads some test data for exploring the
// interactions between types. Make sure it's never true
// during the pull review process
const TEST_MODE = true
if (TEST_MODE) {
  const TEST_NOTEBOOK = require('src/notebooks/context/notebook.mock.json')
  DEFAULT_CONTEXT.id = TEST_NOTEBOOK.id
  DEFAULT_CONTEXT.pipes = TEST_NOTEBOOK.pipes
}

export const NotebookContext = React.createContext<NotebookContextType>(
  DEFAULT_CONTEXT
)

export const NotebookProvider: FC = ({children}) => {
  const [id] = useState(DEFAULT_CONTEXT.id)
  const [pipes, setPipes] = useState(DEFAULT_CONTEXT.pipes)

  function addPipe(pipe: Pipe) {
    setPipes(pipes => {
      pipes.push(pipe)
      return pipes.slice()
    })
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

  function removePipe(idx: number) {
    setPipes(pipes => {
      pipes.splice(idx, 1)
      return pipes.slice()
    })
  }

  return (
    <NotebookContext.Provider
      value={{
        id,
        pipes,
        updatePipe,
        addPipe,
        removePipe,
      }}
    >
      {children}
    </NotebookContext.Provider>
  )
}
