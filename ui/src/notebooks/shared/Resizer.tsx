// Libraries
import React, {FC, useRef, useEffect, ReactNode, useState} from 'react'
import classnames from 'classnames'

// Components
import ResizerHeader from 'src/notebooks/shared/ResizerHeader'

// Types
import {IconFont} from '@influxdata/clockface'
import {PipeData} from 'src/notebooks/index'

// Styles
import 'src/notebooks/shared/Resizer.scss'

export type Visibility = 'visible' | 'hidden'

interface Props {
  data: PipeData
  onUpdate: (data: PipeData) => void
  children: ReactNode
  /** If true the resizer can be toggled between Hidden & Visible */
  toggleVisibilityEnabled: boolean
  /** If true the resizer cannot be resized, have its visibility toggled, and children will be hidden */
  resizingEnabled: boolean
  /** Icon to display in header when resizing is disabled */
  emptyIcon?: IconFont
  /** Text to display when resizing is disabled */
  emptyText: string
  /** Text to display when the resizer is collapsed */
  hiddenText?: string
  /** When resizing is enabled the panel cannot be resized below this amount */
  minimumHeight?: number
  /** Renders this element beneath the visibility toggle in the header */
  additionalControls?: JSX.Element | JSX.Element[]
}

const MINIMUM_RESIZER_HEIGHT = 180

const Resizer: FC<Props> = ({
  data,
  onUpdate,
  children,
  emptyIcon = IconFont.Zap,
  emptyText,
  hiddenText = 'Hidden',
  minimumHeight = MINIMUM_RESIZER_HEIGHT,
  resizingEnabled,
  additionalControls,
  toggleVisibilityEnabled,
}) => {
  const height = data.panelHeight
  const visibility = data.panelVisibility

  const [size, updateSize] = useState<number>(height)
  const [isDragging, updateDragging] = useState<boolean>(false)
  const bodyRef = useRef<HTMLDivElement>(null)
  const dragHandleRef = useRef<HTMLDivElement>(null)

  const bodyClassName = classnames('panel-resizer--body', {
    [`panel-resizer--body__${visibility}`]: resizingEnabled && visibility,
  })

  const updateResultsStyle = (): void => {
    if (bodyRef.current && resizingEnabled && visibility === 'visible') {
      bodyRef.current.setAttribute('style', `height: ${size}px`)
    } else {
      bodyRef.current.setAttribute('style', '')
    }
  }

  const handleUpdateVisibility = (panelVisibility: Visibility): void => {
    onUpdate({panelVisibility})
  }

  const handleUpdateHeight = (panelHeight: number): void => {
    onUpdate({panelHeight})
  }

  // Ensure results renders with proper height on initial render
  useEffect(() => {
    updateResultsStyle()
  }, [])

  // Update results height when associated props change
  useEffect(() => {
    updateResultsStyle()
  }, [size, visibility, resizingEnabled])

  // Update local height when context height changes
  // so long as it is a different value
  useEffect(() => {
    if (height !== size) {
      updateSize(height)
    }
  }, [height])

  // Handle changes in drag state
  useEffect(() => {
    if (isDragging === true) {
      dragHandleRef.current &&
        dragHandleRef.current.classList.add(
          'panel-resizer--drag-handle__dragging'
        )
    }

    if (isDragging === false) {
      dragHandleRef.current &&
        dragHandleRef.current.classList.remove(
          'panel-resizer--drag-handle__dragging'
        )
      handleUpdateHeight(size)
    }
  }, [isDragging])

  const handleMouseMove = (e: MouseEvent): void => {
    if (!bodyRef.current) {
      return
    }

    const {pageY} = e
    const {top} = bodyRef.current.getBoundingClientRect()

    const updatedHeight = Math.round(Math.max(pageY - top, minimumHeight))

    updateSize(updatedHeight)
  }

  const handleMouseDown = (): void => {
    updateDragging(true)
    const body = document.getElementsByTagName('body')[0]
    body && body.classList.add('panel-resizer-dragging')

    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
  }

  const handleMouseUp = (): void => {
    updateDragging(false)
    const body = document.getElementsByTagName('body')[0]
    body && body.classList.remove('panel-resizer-dragging')

    window.removeEventListener('mousemove', handleMouseMove)
    window.removeEventListener('mouseup', handleMouseUp)
  }

  let body = children

  if (!resizingEnabled) {
    body = <div className="panel-resizer--empty">{emptyText}</div>
  }

  if (resizingEnabled && visibility === 'hidden') {
    body = <div className="panel-resizer--empty">{hiddenText}</div>
  }

  return (
    <div className="panel-resizer">
      <ResizerHeader
        emptyIcon={emptyIcon}
        visibility={visibility}
        onStartDrag={handleMouseDown}
        dragHandleRef={dragHandleRef}
        resizingEnabled={resizingEnabled}
        additionalControls={additionalControls}
        onUpdateVisibility={handleUpdateVisibility}
        toggleVisibilityEnabled={toggleVisibilityEnabled}
      />
      <div className={bodyClassName} ref={bodyRef}>
        {body}
      </div>
    </div>
  )
}

export default Resizer
