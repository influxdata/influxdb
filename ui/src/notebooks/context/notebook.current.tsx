import React, {FC, useContext, useEffect, useCallback, useMemo} from 'react'
import createPersistedState from 'use-persisted-state'
import {Notebook, DataID, PipeData} from 'src/notebooks'
import {
  NotebookListContext,
  NotebookListProvider,
} from 'src/notebooks/context/notebook.list'
import {v4 as UUID} from 'uuid'
import {RemoteDataState} from 'src/types'

const useNotebookCurrentState = createPersistedState('current-notebook')

export interface NotebookContextType {
  id: DataID<Notebook> | null
  notebook: Notebook | null
  change: (id: DataID<Notebook>) => void
  add: (data: Partial<PipeData>, index?: number) => string
  update: (notebook: Partial<Notebook>) => void
  remove: () => void
}

export const DEFAULT_CONTEXT: NotebookContextType = {
  id: null,
  notebook: null,
  add: () => '',
  change: () => {},
  update: () => {},
  remove: () => {},
}

export const NotebookContext = React.createContext<NotebookContextType>(
  DEFAULT_CONTEXT
)

let GENERATOR_INDEX = 0

const getHumanReadableName = (type: string): string => {
  ++GENERATOR_INDEX

  switch (type) {
    case 'data':
      return `Data Source ${GENERATOR_INDEX}`
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
  const [currentID, setCurrentID] = useNotebookCurrentState(null)
  //const [currentID, setCurrentID] = useState(null)
  const {notebooks, add, update, remove} = useContext(NotebookListContext)

  const change = useCallback(
    (id: DataID<Notebook>) => {
      if (!notebooks || !notebooks.hasOwnProperty(id)) {
        throw new Error('Notebook does note exist')
      }

      setCurrentID(id)
    },
    [currentID]
  )

  const updateCurrent = useCallback(
    (notebook: Notebook) => {
      update(currentID, notebook)
    },
    [currentID]
  )

  const removeCurrent = useCallback(() => {
    remove(currentID)
  }, [currentID])

  const addPipe = (initial: PipeData, index?: number) => {
    const id = UUID()

    notebooks[currentID].data.add(id, initial)
    notebooks[currentID].meta.add(id, {
      title: getHumanReadableName(initial.type),
      visible: true,
      loading: RemoteDataState.NotStarted,
      focus: false,
    })

    if (typeof index !== 'undefined') {
      notebooks[currentID].data.move(id, index + 1)
    }

    return id
  }

  useEffect(() => {
    if (!currentID) {
      const id = add()
      setCurrentID(id)
      return
    }
  }, [currentID])

  return useMemo(() => {
    if (!notebooks || !notebooks.hasOwnProperty(currentID)) {
      return null
    }

    return (
      <NotebookContext.Provider
        value={{
          id: currentID,
          notebook: notebooks[currentID],
          add: addPipe,
          update: updateCurrent,
          remove: removeCurrent,
          change,
        }}
      >
        {children}
      </NotebookContext.Provider>
    )
  }, [currentID, (notebooks || {})[currentID]])
}

const CurrentNotebook: FC = ({children}) => {
  return (
    <NotebookListProvider>
      <NotebookProvider>{children}</NotebookProvider>
    </NotebookListProvider>
  )
}

export default CurrentNotebook
