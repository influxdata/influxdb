import React, {FC, useState, RefObject} from 'react'
import {DataID, PipeData} from 'src/notebooks'

export interface Ref {
  panel: RefObject<HTMLDivElement>
  focus: boolean
}

export interface RefMap {
  [key: DataID<PipeData>]: Ref
}

export interface RefContextType {
  get: (id: DataID<PipeData>) => Ref
  update: (id: DataID<PipeData>, data: Partial<Ref>) => void
}

export const DEFAULT_CONTEXT: RefContextType = {
  get: () => ({} as Ref),
  update: () => {},
}

export const RefContext = React.createContext<RefContextType>(DEFAULT_CONTEXT)

export const RefProvider: FC = ({children}) => {
  const [refs, setRefs] = useState({})

  const get = (id: DataID<PipeData>) => {
    return (
      refs[id] || {
        panel: null,
        focus: false,
      }
    )
  }
  const update = (id: DataID<PipeData>, data: Partial<Ref>) => {
    refs[id] = {
      ...get(id),
      ...data,
    }
    setRefs({
      ...refs,
    })
  }

  return (
    <RefContext.Provider
      value={{
        get,
        update,
      }}
    >
      {children}
    </RefContext.Provider>
  )
}
