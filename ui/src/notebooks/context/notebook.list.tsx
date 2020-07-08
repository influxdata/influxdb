import React, {FC, useState} from 'react'
//import createPersistedState from 'use-persisted-state'
import {v4 as UUID} from 'uuid'
import {
  NotebookList,
  Notebook,
  NotebookState,
  DataID,
  Resource,
  PipeData,
  PipeMeta,
} from 'src/notebooks'
import useResource from 'src/notebooks/context/resource.hook'

// const useNotebookListState = createPersistedState('notebooks')
/*
() => {
    return (initial) => {
        return [ output, setter ]
    }
}
 */

export interface NotebookListContextType extends NotebookList {
  add: (notebook?: Notebook) => string
  update: (id: DataID<Notebook>, notebook: Notebook) => void
  remove: (id: DataID<Notebook>) => void
}

export const EMPTY_NOTEBOOK: NotebookState = {
  data: {
    byID: {},
    allIDs: [],
  } as Resource<PipeData>,
  meta: {
    byID: {},
    allIDs: [],
  } as Resource<PipeMeta>,
}

export const DEFAULT_CONTEXT: NotebookListContextType = {
  notebooks: {},
  add: (_notebook?: Notebook) => {},
  update: (_id: DataID<Notebook>, _notebook: Notebook) => {},
  remove: (_id: DataID<Notebook>) => {},
} as NotebookListContextType

export const NotebookListContext = React.createContext<NotebookListContextType>(
  DEFAULT_CONTEXT
)

export const NotebookListProvider: FC = ({children}) => {
  //    const [notebooks, setNotebooks] = useNotebookListState(DEFAULT_CONTEXT.notebooks)
  const [notebooks, setNotebooks] = useState(DEFAULT_CONTEXT.notebooks)

  const add = (notebook?: Notebook): string => {
    const id = UUID()
    let _notebook

    if (!notebook) {
      _notebook = {
        ...EMPTY_NOTEBOOK,
      }
    } else {
      _notebook = {
        data: notebook.data.serialize(),
        meta: notebook.meta.serialize(),
      }
    }

    setNotebooks({
      ...notebooks,
      [id]: _notebook,
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
        data: notebook.data.serialize(),
        meta: notebook.meta.serialize(),
      },
    })
  }

  const remove = (id: DataID<Notebook>) => {
    const _notebooks = {
      ...notebooks,
    }

    delete _notebooks[id]

    setNotebooks(_notebooks)
  }

  const notebookList = Object.keys(notebooks).reduce((acc, curr) => {
    const stateUpdater = (field, data) => {
      const _notebook = {
        ...notebooks[curr],
      }

      _notebook[field] = data

      setNotebooks({
        ...notebooks,
        [curr]: _notebook,
      })
    }

    acc[curr] = {
      data: useResource(notebooks[curr].data, data => {
        stateUpdater('data', data)
      }),
      meta: useResource(notebooks[curr].meta, data => {
        stateUpdater('meta', data)
      }),
    } as Notebook

    return acc
  }, {})

  return (
    <NotebookListContext.Provider
      value={{
        notebooks: notebookList,
        add,
        update,
        remove,
      }}
    >
      {children}
    </NotebookListContext.Provider>
  )
}

export default NotebookListProvider
