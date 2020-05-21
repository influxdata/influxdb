import React, {FC, useState, useCallback} from 'react'
import {AutoRefresh, TimeRange} from 'src/types'
import {DEFAULT_TIME_RANGE} from 'src/shared/constants/timeRanges'
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

export interface TimeBlock {
  range: TimeRange
  refresh: AutoRefresh
}

export interface TimeState {
  [key: string]: TimeBlock
}

export const DEFAULT_STATE: TimeBlock = {
  range: DEFAULT_TIME_RANGE,
  refresh: AUTOREFRESH_DEFAULT,
}

export interface TimeContext {
  timeContext: TimeState
  addTimeContext: (id: string, block?: TimeBlock) => void
  updateTimeContext: (id: string, block: TimeBlock) => void
  removeTimeContext: (id: string) => void
}

export const DEFAULT_CONTEXT: TimeContext = {
  timeContext: {},
  addTimeContext: () => {},
  updateTimeContext: () => {},
  removeTimeContext: () => {},
}

export const TimeContext = React.createContext<TimeContext>(DEFAULT_CONTEXT)

export const TimeProvider: FC = ({children}) => {
  const [timeContext, setTimeContext] = useState({})

  const addTimeContext = useCallback((id: string, block?: TimeBlock) => {
    setTimeContext(ranges => {
      if (ranges.hasOwnProperty(id)) {
        throw new Error(
          `TimeContext[${id}] already exists: use updateContext instead`
        )
        return ranges
      }

      return {
        ...ranges,
        [id]: {...(block || DEFAULT_STATE)},
      }
    })
  })

  const updateTimeContext = useCallback((id: string, block: TimeBlock) => {
    setTimeContext(ranges => {
      return {
        ...ranges,
        [id]: {
          ...(ranges[id] || {}),
          ...block,
        },
      }
    })
  })

  const removeTimeContext = useCallback((id: string) => {
    setTimeContext(ranges => {
      if (!ranges.hasOwnProperty(id)) {
        throw new Error(`TimeContext[${id}] doesn't exist`)
        return ranges
      }
      delete ranges[id]
      return {...ranges}
    })
  })

  return (
    <TimeContext.Provider
      value={{
        timeContext,
        addTimeContext,
        updateTimeContext,
        removeTimeContext,
      }}
    >
      {children}
    </TimeContext.Provider>
  )
}
