// Libraries
import React, {FC, useState, RefObject} from 'react'

export interface ScrollContextType {
  scrollPosition: number
  scrollToPipe: (panelRef: RefObject<HTMLDivElement>) => void
}

export const DEFAULT_CONTEXT: ScrollContextType = {
  scrollPosition: 0,
  scrollToPipe: () => {},
}

export const ScrollContext = React.createContext<ScrollContextType>(
  DEFAULT_CONTEXT
)

export const ScrollProvider: FC = ({children}) => {
  const [scrollPosition, setListScrollPosition] = useState(
    DEFAULT_CONTEXT.scrollPosition
  )

  const scrollToPipe = (panelRef: RefObject<HTMLDivElement>) => {
    if (panelRef && panelRef.current) {
      const {offsetTop} = panelRef.current
      setListScrollPosition(offsetTop)
    }
  }

  return (
    <ScrollContext.Provider
      value={{
        scrollPosition,
        scrollToPipe,
      }}
    >
      {children}
    </ScrollContext.Provider>
  )
}
