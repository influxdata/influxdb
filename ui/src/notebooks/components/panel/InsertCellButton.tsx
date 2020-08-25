// Libraries
import React, {FC, useRef, useContext} from 'react'

// Components
import {
  Popover,
  Appearance,
  ComponentColor,
  ComponentSize,
  SquareButton,
  IconFont,
  FlexBox,
  FlexDirection,
  AlignItems,
  PopoverPosition,
} from '@influxdata/clockface'
import AddButtons from 'src/notebooks/components/AddButtons'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

// Styles
import 'src/notebooks/components/panel/InsertCellButton.scss'

interface Props {
  id: string
}

const InsertCellButton: FC<Props> = ({id}) => {
  const {notebook} = useContext(NotebookContext)
  const dividerRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)
  const popoverVisible = useRef<boolean>(false)
  const index = notebook.data.indexOf(id)

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
    <div className="notebook-divider" ref={dividerRef}>
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
        position={PopoverPosition.Below}
        onShow={handlePopoverShow}
        onHide={handlePopoverHide}
        contents={onHide => (
          <FlexBox
            direction={FlexDirection.Column}
            alignItems={AlignItems.Stretch}
            margin={ComponentSize.Small}
            className="insert-cell-menu"
          >
            <p className="insert-cell-menu--title">Insert Cell Here</p>
            <AddButtons
              index={index}
              onInsert={onHide}
              eventName="Notebook Insert Cell Inline"
            />
          </FlexBox>
        )}
      />
    </div>
  )
}

export default InsertCellButton
