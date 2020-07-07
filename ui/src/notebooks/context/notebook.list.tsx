import React, {FC} from 'react'
import createPersistedState from 'use-persisted-state'
import { v4 as UUID } from 'uuid'
import {NotebookList, Notebook, DataID} from 'src/notebooks'

const useNotebookListState = createPersistedState('notebooks')

export interface NotebookListContextType extends NotebookList {
    add: (notebook: Notebook) => string
    update: (id: DataID<Notebook>, notebook: Notebook) => void
    remove: (id: DataID<Notebook>) => void
}

export const EMPTY_NOTEBOOK:Notebook = {
    data: {
        byID: {},
        allIDs: []
    },
    meta: {
        byID: {},
        allIDs: []
    },
    order: []
}

export const DEFAULT_CONTEXT:NotebookListContextType = {
    notebooks: {},
    add: () => {},
    update: () => {},
    remove: () => {}
} as NotebookListContextType

export const NotebookListContext = React.createContext<NotebookListContextType>(
    DEFAULT_CONTEXT
)

export const NotebookListProvider: FC = ({children}) => {
    const [notebooks, setNotebooks] = useNotebookListState(DEFAULT_CONTEXT.notebooks)

    const add = (notebook?: Notebook):string => {
        const id = UUID()

        if (!notebook) {
            notebook = {
                ...EMPTY_NOTEBOOK
            }
        }

        setNotebooks({
            ...notebooks,
            [id]: notebook
        })

        return id
    }

    const update = (id: DataID<Notebook>, notebook: Notebook) => {
        if (!notebooks.hasOwnProperty(id)) {
            throw new Error('Notebook not found')
        }

        console.log('updating', id, {...notebooks[id], ...notebook})
        //console.trace()

        setNotebooks({
            ...notebooks,
            [id]: {
                ...notebooks[id],
                ...notebook
            }
        })
    }

    const remove = (id: DataID<Notebook>) => {
        const _notebooks = {
            ...notebooks
        }

        delete _notebooks[id]

        setNotebooks(_notebooks)
    }

  return (
    <NotebookListContext.Provider
      value={{
          notebooks,
          add,
          update,
          remove
      }}
    >
      {children}
    </NotebookListContext.Provider>
  )
}

export default NotebookListProvider

