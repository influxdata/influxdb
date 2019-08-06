// Libraries
import React, {FC, MouseEvent} from 'react'

// Components
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  SquareButton,
  ComponentColor,
  ComponentSize,
  IconFont,
  Page,
} from '@influxdata/clockface'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'
import CheckAlertingButton from 'src/alerting/components/CheckAlertingButton'

// Constants
import {DEFAULT_CHECK_NAME, CHECK_NAME_MAX_LENGTH} from 'src/alerting/constants'

interface Props {
  name: string
  onSetName: (name: string) => void
  onCancel: () => void
  onSave: () => void
}

const saveButtonClass = 'veo-header--save-cell-button'

const CheckEOHeader: FC<Props> = ({name, onSetName, onCancel, onSave}) => {
  const handleClickOutsideTitle = (e: MouseEvent<HTMLElement>) => {
    const target = e.target as HTMLButtonElement

    if (!target.className.includes(saveButtonClass)) {
      return
    }

    onSave()
  }

  return (
    <div className="veo-header">
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <RenamablePageTitle
            name={name}
            onRename={onSetName}
            placeholder={DEFAULT_CHECK_NAME}
            maxLength={CHECK_NAME_MAX_LENGTH}
            onClickOutside={handleClickOutsideTitle}
          />
        </Page.Header.Left>
        <Page.Header.Right>
          <CheckAlertingButton />
          <ViewTypeDropdown />
          <VisOptionsButton />
          <SquareButton
            icon={IconFont.Remove}
            onClick={onCancel}
            size={ComponentSize.Small}
          />
          <SquareButton
            className={saveButtonClass}
            icon={IconFont.Checkmark}
            color={ComponentColor.Success}
            size={ComponentSize.Small}
            onClick={onSave}
            testID="save-cell--button"
          />
        </Page.Header.Right>
      </Page.Header>
    </div>
  )
}

export default CheckEOHeader
