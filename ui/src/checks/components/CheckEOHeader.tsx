// Libraries
import React, {useState, FC, MouseEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  SquareButton,
  ComponentSize,
  ComponentStatus,
  IconFont,
  Page,
} from '@influxdata/clockface'
import CheckAlertingButton from 'src/checks/components/CheckAlertingButton'
import CheckEOSaveButton from 'src/checks/components/CheckEOSaveButton'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {
  isCheckSaveable,
  isDraftQueryAlertable,
} from 'src/timeMachine/utils/queryBuilder'

// Actions
import {setActiveTab} from 'src/timeMachine/actions'

// Constants
import {DEFAULT_CHECK_NAME, CHECK_NAME_MAX_LENGTH} from 'src/alerting/constants'

// Types
import {RemoteDataState, AppState} from 'src/types'
import {
  createCheckFromTimeMachine,
  updateCheckFromTimeMachine,
} from 'src/checks/actions/thunks'

interface OwnProps {
  name: string
  onSetName: (name: string) => void
  onCancel: () => void
  onSave: typeof createCheckFromTimeMachine | typeof updateCheckFromTimeMachine
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const saveButtonClass = 'veo-header--save-cell-button'

const CheckEOHeader: FC<Props> = ({
  name,
  onSetName,
  onCancel,
  onSave,
  setActiveTab,
  activeTab,
  draftQueries,
  checkType,
  thresholds,
}) => {
  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)

  const handleSave = () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    setSaveStatus(RemoteDataState.Loading)
    onSave()
    setSaveStatus(RemoteDataState.NotStarted)
  }

  const handleClickOutsideTitle = (e: MouseEvent<HTMLElement>) => {
    if ((e.target as Element).classList.contains(saveButtonClass)) {
      handleSave()
    }
  }

  const saveButtonStatus = () => {
    if (!isCheckSaveable(draftQueries, checkType, thresholds)) {
      return ComponentStatus.Disabled
    }

    if (saveStatus == RemoteDataState.Loading) {
      return ComponentStatus.Loading
    }

    return ComponentStatus.Default
  }

  const {singleField, singleAggregateFunc} = isDraftQueryAlertable(draftQueries)
  const oneOrMoreThresholds =
    checkType === 'threshold' && thresholds && !!thresholds.length

  return (
    <>
      <Page.Header fullWidth={true}>
        <RenamablePageTitle
          name={name}
          onRename={onSetName}
          placeholder={DEFAULT_CHECK_NAME}
          maxLength={CHECK_NAME_MAX_LENGTH}
          onClickOutside={handleClickOutsideTitle}
        />
      </Page.Header>
      <Page.ControlBar fullWidth={true}>
        <Page.ControlBarLeft>
          {activeTab !== 'customCheckQuery' && (
            <CheckAlertingButton
              activeTab={activeTab}
              draftQueries={draftQueries}
              setActiveTab={setActiveTab}
            />
          )}
        </Page.ControlBarLeft>
        <Page.ControlBarRight>
          <SquareButton
            icon={IconFont.Remove}
            onClick={onCancel}
            size={ComponentSize.Small}
          />
          <CheckEOSaveButton
            status={saveButtonStatus()}
            onSave={handleSave}
            className={saveButtonClass}
            checkType={checkType}
            singleField={singleField}
            singleAggregateFunc={singleAggregateFunc}
            oneOrMoreThresholds={oneOrMoreThresholds}
          />
        </Page.ControlBarRight>
      </Page.ControlBar>
    </>
  )
}

const mstp = (state: AppState) => {
  const {activeTab, draftQueries} = getActiveTimeMachine(state)
  const {
    alertBuilder: {type, thresholds},
  } = state

  return {activeTab, draftQueries, checkType: type, thresholds}
}

const mdtp = {
  setActiveTab: setActiveTab,
}

const connector = connect(mstp, mdtp)

export default connector(CheckEOHeader)
