// Libraries
import React, {useState, FC, MouseEvent} from 'react'

// Components
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  SquareButton,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  IconFont,
  Page,
} from '@influxdata/clockface'
import CheckAlertingButton from 'src/alerting/components/CheckAlertingButton'

// Constants
import {DEFAULT_CHECK_NAME, CHECK_NAME_MAX_LENGTH} from 'src/alerting/constants'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  name: string
  onSetName: (name: string) => void
  onCancel: () => void
  onSave: () => Promise<void>
}

const saveButtonClass = 'veo-header--save-cell-button'

const CheckEOHeader: FC<Props> = ({name, onSetName, onCancel, onSave}) => {
  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)

  const handleSave = async () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    setSaveStatus(RemoteDataState.Loading)
    await onSave()
    setSaveStatus(RemoteDataState.NotStarted)
  }

  const handleClickOutsideTitle = (e: MouseEvent<HTMLElement>) => {
    if ((e.target as Element).classList.contains(saveButtonClass)) {
      handleSave()
    }
  }

  const saveButtonStatus =
    saveStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
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
      <Page.Header.Center widthPixels={300}>
        <CheckAlertingButton />
      </Page.Header.Center>
      <Page.Header.Right>
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
          status={saveButtonStatus}
          onClick={handleSave}
          testID="save-cell--button"
        />
      </Page.Header.Right>
    </Page.Header>
  )
}

export default CheckEOHeader
