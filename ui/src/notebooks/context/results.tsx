import React, {FC, useContext, useEffect, useState} from 'react'
import {FluxResult, Resource, ResourceManipulator} from 'src/notebooks'
import useResource from 'src/notebooks/context/resource.hook'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

export type ResultsContextType = ResourceManipulator<FluxResult>

const EMPTY_STATE = {
  byID: {},
  allIDs: [],
} as Resource<FluxResult>

const DEFAULT_CONTEXT: ResultsContextType = {
  get: _id => null as FluxResult,
  add: (_id, _data) => {},
  update: (_id, _data) => {},
  remove: _id => {},
  indexOf: _id => -1,
  move: _id => {},
  serialize: () => ({...EMPTY_STATE}),

  allIDs: [],
  all: [],
} as ResultsContextType

export const ResultsContext = React.createContext<ResultsContextType>(
  DEFAULT_CONTEXT
)

export const ResultsProvider: FC = ({children}) => {
  const {id, notebook} = useContext(NotebookContext)

  const [results, setResults] = useState({...EMPTY_STATE})

  useEffect(() => {
    setResults({...EMPTY_STATE})
  }, [id])

  const manipulator = useResource<FluxResult>(results, setResults)

  const value = {
    ...manipulator,
    add: (id: string, result?: FluxResult) => {
      try {
        if (result) {
          manipulator.add(id, result)
          return
        }
        const ref = notebook.data.allIDs
        const index = notebook.data.indexOf(id)
        const lastOne = ref[index - 1]
        manipulator.add(id, manipulator.get(ref[index - 1]))
        notebook.meta.update(id, {loading: notebook.meta.get(lastOne).loading})
      } catch (_e) {
        // swallow that
      }
    },
  }

  return (
    <ResultsContext.Provider value={value}>{children}</ResultsContext.Provider>
  )
}
