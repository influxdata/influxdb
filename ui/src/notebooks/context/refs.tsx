import React, {FC, useState, RefObject} from 'react'

export interface Ref {
  panel: RefObject<HTMLDivElement>
  focus: boolean
}

export interface RefMap {
  [key: DataID<PipeData>]: Ref
}

export interface RefContextType {
  get: (id: string) => Ref
  update: (id: string, data: Partial<Ref>) => void
}

export const DEFAULT_CONTEXT: RefContextType = {
  get: () => ({} as Ref),
  update: () => {},
}

export const RefContext = React.createContext<RefContextType>(DEFAULT_CONTEXT)

export const RefProvider: FC = ({children}) => {
  const [refs, setRefs] = useState({})

  const get = (id: string) => {
    return (
      refs[id] || {
        panel: null,
        focus: false,
      }
    )
  }
  const update = (id: string, data: Partial<Ref>) => {
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
