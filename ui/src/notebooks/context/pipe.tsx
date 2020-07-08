import React, {FC, useContext} from 'react'
import {DataID, PipeData, FluxResult} from 'src/notebooks'
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {RemoteDataState} from 'src/types'

export interface PipeContextType {
  data: PipeData
  update: (data: PipeData) => void
  loading: RemoteDataState
  results: FluxResult
}

export const DEFAULT_CONTEXT: PipeContextType = {
  data: {},
  update: () => {},
  loading: RemoteDataState.NotStarted,
  results: {
    source: '',
    raw: '',
    parsed: {},
  } as FluxResult,
}

export const PipeContext = React.createContext<PipeContextType>(DEFAULT_CONTEXT)

interface PipeContextProps {
  id: DataID<PipeData>
}

export const PipeProvider: FC<PipeContextProps> = ({id, children}) => {
  const {notebook} = useContext(NotebookContext)
  const updater = (_data: PipeData) => {
    notebook.data.update(id, _data)
  }

  //TODO add results in here too
  return (
    <PipeContext.Provider
      value={{
        data: notebook.data.get(id),
        update: updater,
        results: notebook.results,
        loading: notebook.meta.get(id).loading,
      }}
    >
      {children}
    </PipeContext.Provider>
  )
}
