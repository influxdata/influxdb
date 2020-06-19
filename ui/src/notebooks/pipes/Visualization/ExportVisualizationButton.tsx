// Libraries
import React, {FC, useRef, useState} from 'react'

// Components
import {
  SquareButton,
  ButtonRef,
  IconFont,
  ComponentStatus,
  Popover,
  Appearance,
  PopoverPosition,
} from '@influxdata/clockface'

interface Props {
  disabled: boolean
  children: (onHide: () => void) => JSX.Element
}

const ExportVisualizationButton: FC<Props> = ({disabled, children}) => {
  const [isExporting, setIsExporting] = useState<boolean>(false)
  const triggerRef = useRef<ButtonRef>(null)
  const status = disabled ? ComponentStatus.Disabled : ComponentStatus.Default

  const handleShow = (): void => {
    setIsExporting(true)
  }

  const handleHide = (): void => {
    setIsExporting(false)
  }

  const buttonStyle = isExporting ? {opacity: 1} : null

  return (
    <>
      <SquareButton
        ref={triggerRef}
        icon={IconFont.Export}
        titleText="Save to Dashboard"
        status={status}
        style={buttonStyle}
      />
      <Popover
        triggerRef={triggerRef}
        contents={children}
        appearance={Appearance.Outline}
        enableDefaultStyles={false}
        position={PopoverPosition.ToTheLeft}
        onShow={handleShow}
        onHide={handleHide}
      />
    </>
  )
}

export default ExportVisualizationButton
