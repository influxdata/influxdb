import React, {FC, useState} from 'react'

// TODO make this polymorphic too
export type Pipe = any

export interface NotebookContext {
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

// just to see if it's all working
const TEST_NOTEBOOK = {
  id: 'testing',
  pipes: [
    {
      type: 'query',
      activeQuery: 0,
      queries: [
        {
          text: 'this is just a test',
          editMode: 'advanced',
          builderConfig: {
            buckets: [],
            tags: [],
            functions: [],
          },
        },
      ],
    },
  ],
}

export const NotebookContext = React.createContext<NotebookContext>(
  DEFAULT_CONTEXT
)

export const NotebookProvider: FC = ({children}) => {
  const [id, setID] = useState(TEST_NOTEBOOK.id)
  const [pipes, setPipes] = useState(TEST_NOTEBOOK.pipes)

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
