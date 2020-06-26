// Libraries
import React, {FC, useRef, useEffect} from 'react'

// Components
import {
  Popover,
  Appearance,
  ComponentColor,
  SquareButton,
  IconFont,
} from '@influxdata/clockface'
import InsertCellMenu from 'src/notebooks/components/panel/InsertCellMenu'

// Styles
import 'src/notebooks/components/panel/InsertCellButton.scss'

interface Props {
  index: number
}

const InsertCellButton: FC<Props> = ({index}) => {
  const dividerRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const popoverVisible = useRef<boolean>(false)
  const buttonPositioningEnabled = useRef<boolean>(false)

  useEffect(() => {
    window.addEventListener('mousemove', handleMouseMove)

    return () => {
      window.removeEventListener('mousemove', handleMouseMove)
    }
  }, [])

  const handleMouseMove = (e: MouseEvent): void => {
    if (!dividerRef.current || !buttonRef.current) {
      return
    }

    if (
      popoverVisible.current === false &&
      buttonPositioningEnabled.current === true
    ) {
      const {pageX} = e
      const {left, width} = dividerRef.current.getBoundingClientRect()

      const minLeft = 0
      const maxLeft = width

      const buttonLeft = Math.min(Math.max(pageX - left, minLeft), maxLeft)
      buttonRef.current.setAttribute('style', `left: ${buttonLeft}px`)
    }
  }

  const handleMouseEnter = () => {
    buttonPositioningEnabled.current = true
  }

  const handleMouseLeave = () => {
    buttonPositioningEnabled.current = false
  }

  const handlePopoverShow = () => {
    popoverVisible.current = true
    dividerRef.current &&
      dividerRef.current.classList.add('notebook-divider__popped')
  }

  const handlePopoverHide = () => {
    popoverVisible.current = false
    dividerRef.current &&
      dividerRef.current.classList.remove('notebook-divider__popped')
  }

  return (
    <div
      className="notebook-divider"
      ref={dividerRef}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
    >
      <SquareButton
        icon={IconFont.Plus}
        ref={buttonRef}
        className="notebook-divider--button"
        color={ComponentColor.Secondary}
        active={popoverVisible.current}
      />
      <Popover
        enableDefaultStyles={false}
        appearance={Appearance.Outline}
        color={ComponentColor.Secondary}
        triggerRef={buttonRef}
        contents={onHide => <InsertCellMenu index={index} onInsert={onHide} />}
        onShow={handlePopoverShow}
        onHide={handlePopoverHide}
      />
    </div>
  )
}

export default InsertCellButton
