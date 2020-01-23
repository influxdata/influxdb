import React, {useState, FC} from 'react'

// Components
import {
  ComponentColor,
  Form,
  Button,
  ComponentStatus,
  Grid,
  Columns,
} from '@influxdata/clockface'

// Utils
import {useRuleState} from './RuleOverlayProvider'

import {NotificationRuleDraft, RemoteDataState} from 'src/types'

interface Props {
  saveButtonText: string
  onSave: (draftRule: NotificationRuleDraft) => Promise<void>
}

const RuleOverlayFooter: FC<Props> = ({saveButtonText, onSave}) => {
  const rule = useRuleState()

  const [saveStatus, setSaveStatus] = useState(RemoteDataState.NotStarted)
  const [errorMessage, setErrorMessage] = useState<string>(null)

  const handleSave = async () => {
    if (saveStatus === RemoteDataState.Loading) {
      return
    }

    try {
      setSaveStatus(RemoteDataState.Loading)
      setErrorMessage(null)

      await onSave(rule)
    } catch (e) {
      setSaveStatus(RemoteDataState.Error)
      setErrorMessage(e.message)
    }
  }

  const buttonStatus =
    saveStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

  return (
    <>
      <Grid.Row>
        <Grid.Column widthXS={Columns.Twelve}>
          {errorMessage && (
            <div className="rule-overlay-footer--error">{errorMessage}</div>
          )}
          <Form.Footer className="rule-overlay-footer">
            <Button
              testID="rule-overlay-save--button"
              text={saveButtonText}
              onClick={handleSave}
              color={ComponentColor.Primary}
              status={buttonStatus}
            />
          </Form.Footer>
        </Grid.Column>
      </Grid.Row>
    </>
  )
}

export default RuleOverlayFooter
