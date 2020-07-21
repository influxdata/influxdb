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

// Utils
import {event} from 'src/cloud/utils/reporting'

interface Props {
  disabled: boolean
  children: (onHide: () => void) => JSX.Element
}

const ExportVisualizationButton: FC<Props> = ({disabled, children}) => {
  const [isExporting, setIsExporting] = useState<boolean>(false)
  const triggerRef = useRef<ButtonRef>(null)
  const status = disabled ? ComponentStatus.Disabled : ComponentStatus.Default

  const handleShow = (): void => {
    event('Export Notebook Button Clicked')
    setIsExporting(true)
  }

  const handleHide = (): void => {
    event('Export Notebook Canceled')
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
        className="flows-export-visualization-button"
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
