// Libraries
import React, {FC, useRef, useEffect, ReactNode} from 'react'

// Components
import ResultsHeader from 'src/notebooks/pipes/Query/ResultsHeader'

// Types
import {ResultsVisibility} from 'src/notebooks/pipes/Query'

interface Props {
  height: number
  onUpdateHeight: (height: number) => void
  children: ReactNode
  visibility: ResultsVisibility
  onUpdateVisibility: (visibility: ResultsVisibility) => void
  resizingEnabled: boolean
}

const MINIMUM_RESULTS_PANEL_HEIGHT = 100

const ResultsResizer: FC<Props> = ({
  height,
  onUpdateHeight,
  children,
  visibility,
  onUpdateVisibility,
  resizingEnabled,
}) => {
  const resultsBodyRef = useRef<HTMLDivElement>(null)
  const dragHandleRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (resultsBodyRef.current && resizingEnabled && visibility === 'visible') {
      resultsBodyRef.current.setAttribute('style', `height: ${height}px`)
    } else if (
      resultsBodyRef.current &&
      resizingEnabled &&
      visibility === 'hidden'
    ) {
      resultsBodyRef.current.setAttribute('style', undefined)
    }
  }, [height, visibility, resizingEnabled])

  const handleMouseMove = (e: MouseEvent): void => {
    if (!resultsBodyRef.current) {
      return
    }

    const {pageY} = e
    const {top} = resultsBodyRef.current.getBoundingClientRect()

    const updatedHeight = Math.max(pageY - top, MINIMUM_RESULTS_PANEL_HEIGHT)

    onUpdateHeight(updatedHeight)
  }

  const handleMouseDown = (): void => {
    if (dragHandleRef.current) {
      dragHandleRef.current.classList.add(
        'notebook-raw-data--drag-handle__dragging'
      )
    }
    const body = document.getElementsByTagName('body')[0]
    body && body.classList.add('notebook-results--dragging')

    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
  }

  const handleMouseUp = (): void => {
    if (dragHandleRef.current) {
      dragHandleRef.current.classList.remove(
        'notebook-raw-data--drag-handle__dragging'
      )
    }
    const body = document.getElementsByTagName('body')[0]
    body && body.classList.remove('notebook-results--dragging')

    window.removeEventListener('mousemove', handleMouseMove)
    window.removeEventListener('mouseup', handleMouseUp)
  }

  return (
    <>
      <ResultsHeader
        resizingEnabled={resizingEnabled}
        visibility={visibility}
        onUpdateVisibility={onUpdateVisibility}
        onStartDrag={handleMouseDown}
        dragHandleRef={dragHandleRef}
      />
      <div className="notebook-raw-data--body" ref={resultsBodyRef}>
        {children}
      </div>
    </>
  )
}

export default ResultsResizer
