// Libraries
import React, {FC, useState} from 'react'

// Constants
import {NOTEBOOK_PANEL_ID} from 'src/notebooks/constants'

export interface ScrollContextType {
  scrollPosition: number
  scrollToPipe: (idx: number) => void
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

  const scrollToPipe = (idx: number) => {
    const targetPipe = document.getElementById(`${NOTEBOOK_PANEL_ID}${idx}`)

    if (targetPipe) {
      const {offsetTop} = targetPipe
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
