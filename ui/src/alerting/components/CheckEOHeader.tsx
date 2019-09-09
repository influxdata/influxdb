// Libraries
import React, {useState, FC, MouseEvent} from 'react'
import {connect} from 'react-redux'

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

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {isCheckSaveable} from 'src/timeMachine/utils/queryBuilder'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Constants
import {DEFAULT_CHECK_NAME, CHECK_NAME_MAX_LENGTH} from 'src/alerting/constants'

// Types
import {
  TimeMachineTab,
  RemoteDataState,
  AppState,
  DashboardDraftQuery,
} from 'src/types'

interface OwnProps {
  name: string
  onSetName: (name: string) => void
  onCancel: () => void
  onSave: () => Promise<void>
}

interface StateProps {
  activeTab: TimeMachineTab
  draftQueries: DashboardDraftQuery[]
}

interface DispatchProps {
  setActiveTab: typeof setActiveTab
}

type Props = OwnProps & StateProps & DispatchProps

const saveButtonClass = 'veo-header--save-cell-button'

const CheckEOHeader: FC<Props> = ({
  name,
  onSetName,
  onCancel,
  onSave,
  setActiveTab,
  activeTab,
  draftQueries,
}) => {
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

  const saveButtonStatus = () => {
    if (!isCheckSaveable(draftQueries)) {
      return ComponentStatus.Disabled
    }

    if (saveStatus == RemoteDataState.Loading) {
      return ComponentStatus.Loading
    }

    return ComponentStatus.Default
  }

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
        <CheckAlertingButton
          activeTab={activeTab}
          draftQueries={draftQueries}
          setActiveTab={setActiveTab}
        />
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
          status={saveButtonStatus()}
          onClick={handleSave}
          testID="save-cell--button"
        />
      </Page.Header.Right>
    </Page.Header>
  )
}

const mstp = (state: AppState): StateProps => {
  const {activeTab, draftQueries} = getActiveTimeMachine(state)

  return {activeTab, draftQueries}
}

const mdtp: DispatchProps = {
  setActiveTab: setActiveTab,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(CheckEOHeader)
