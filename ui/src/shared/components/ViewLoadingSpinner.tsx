// Libraries
import React, {FunctionComponent, useState, useEffect} from 'react'
import {round} from 'lodash'
import classnames from 'classnames'

// Types
import {RemoteDataState} from 'src/types'

// Components
import {TechnoSpinner, ComponentSize} from '@influxdata/clockface'

interface Props {
  loading: RemoteDataState
}

const ViewLoadingSpinner: FunctionComponent<Props> = ({loading}) => {
  const [timerActive, setTimerActive] = useState<boolean>(false)
  const [seconds, setSeconds] = useState<number>(0)

  const timerElementClass = classnames('view-loading-spinner--timer', {
    visible: seconds > 5,
  })

  const resetTimer = (): void => {
    setSeconds(0)
    setTimerActive(false)
  }

  useEffect(() => {
    if (loading === RemoteDataState.Done || RemoteDataState.Error) {
      resetTimer()
    }

    if (loading === RemoteDataState.Loading) {
      setTimerActive(true)
    }
  }, [loading])

  useEffect(() => {
    let interval = null
    if (timerActive) {
      interval = setInterval(() => {
        setSeconds(seconds => seconds + 0.1)
      }, 100)
    } else if (!timerActive && seconds !== 0) {
      clearInterval(interval)
    }
    return () => clearInterval(interval)
  }, [timerActive, seconds])

  if (loading === RemoteDataState.Loading) {
    return (
      <div className="view-loading-spinner">
        <TechnoSpinner diameterPixels={66} strokeWidth={ComponentSize.Medium} />
        <div className={timerElementClass}>{`${round(seconds, 1)}s`}</div>
      </div>
    )
  }

  return null
}

export default ViewLoadingSpinner
