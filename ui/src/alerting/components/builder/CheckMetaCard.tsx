// Libraries
import React, {FC, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Radio,
  ButtonShape,
  Form,
  InputType,
  ComponentSize,
  TextArea,
  AutoComplete,
  Wrap,
} from '@influxdata/clockface'
import {Input} from '@influxdata/clockface'

// Actions
import {updateTimeMachineCheck, changeCheckType} from 'src/timeMachine/actions'

//Selectors
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Check, AppState, CheckType} from 'src/types'
import {
  DEFAULT_CHECK_EVERY,
  DEFAULT_CHECK_OFFSET,
  DEFAULT_CHECK_CRON,
} from 'src/alerting/constants'

interface DispatchProps {
  updateTimeMachineCheck: typeof updateTimeMachineCheck
  changeCheckType: typeof changeCheckType
}

interface StateProps {
  check: Partial<Check>
}

type Props = DispatchProps & StateProps

const CheckMetaCard: FC<Props> = ({
  updateTimeMachineCheck,
  changeCheckType,
  check,
}) => {
  const handleChangeType = (type: CheckType) => {
    changeCheckType(type)
  }

  const handleChangeName = (e: React.ChangeEvent<HTMLInputElement>) => {
    updateTimeMachineCheck({name: e.target.value})
  }

  const handleChangeMessage = (e: ChangeEvent<HTMLTextAreaElement>) => {
    const statusMessageTemplate = e.target.value
    updateTimeMachineCheck({statusMessageTemplate})
  }

  const handleChangeSchedule = (scheduleType: 'cron' | 'every') => {
    if (scheduleType == 'cron' && !check.cron) {
      updateTimeMachineCheck({
        cron: DEFAULT_CHECK_CRON,
        every: null,
        offset: null,
      })
      return
    }
    if (scheduleType == 'every' && !check.every) {
      updateTimeMachineCheck({
        every: DEFAULT_CHECK_EVERY,
        offset: DEFAULT_CHECK_OFFSET,
        cron: null,
      })
      return
    }
  }
  return (
    <>
      <Form.Element label="Check Type">
        <Radio shape={ButtonShape.StretchToFit}>
          <Radio.Button
            key="threshold"
            id="threshold"
            titleText="threshold"
            value="threshold"
            active={check.type === 'threshold'}
            onClick={handleChangeType}
          >
            Threshold
          </Radio.Button>
          <Radio.Button
            key="deadman"
            id="deadman"
            titleText="deadman"
            value="deadman"
            active={check.type === 'deadman'}
            onClick={handleChangeType}
          >
            Deadman
          </Radio.Button>
        </Radio>
      </Form.Element>
      <Form.Element label="Name">
        <Input
          autoFocus={true}
          maxLength={24}
          name="Name"
          onChange={handleChangeName}
          placeholder="Name this check"
          size={ComponentSize.Small}
          spellCheck={false}
          testID="input-field"
          titleText="Title Text"
          type={InputType.Text}
          value={check.name}
        />
      </Form.Element>
      <Form.Element
        label="Status Message Template"
        className="alert-builder--message-template"
      >
        <TextArea
          autoFocus={false}
          autocomplete={AutoComplete.Off}
          form=""
          maxLength={50}
          minLength={5}
          name=""
          onChange={handleChangeMessage}
          placeholder="Placeholder Text"
          readOnly={false}
          required={false}
          size={ComponentSize.Medium}
          spellCheck={false}
          testID="textarea"
          value={check.statusMessageTemplate}
          wrap={Wrap.Soft}
        />
      </Form.Element>
      <Form.Element label="Schedule">
        <Radio shape={ButtonShape.StretchToFit}>
          <Radio.Button
            key="every"
            id="every"
            titleText="every"
            value="every"
            active={!!check.every}
            onClick={handleChangeSchedule}
          >
            Every
          </Radio.Button>
          <Radio.Button
            key="cron"
            id="cron"
            titleText="cron"
            value="cron"
            active={!check.every}
            onClick={handleChangeSchedule}
          >
            Cron
          </Radio.Button>
        </Radio>
      </Form.Element>
      {check.every != null && (
        <Form.Element label="Every">
          <Input
            autoFocus={false}
            maxLength={24}
            name="Name"
            onChange={handleChangeName}
            placeholder="Name this check"
            size={ComponentSize.Small}
            spellCheck={false}
            testID="input-field"
            titleText="Title Text"
            type={InputType.Text}
            value={check.every}
          />
        </Form.Element>
      )}
      {check.offset != null && (
        <Form.Element label="Offset">
          <Input
            autoFocus={false}
            maxLength={24}
            name="Offset"
            onChange={handleChangeName}
            placeholder="offset"
            size={ComponentSize.Small}
            spellCheck={false}
            testID="input-field"
            titleText="Title Text"
            type={InputType.Text}
            value={check.offset}
          />
        </Form.Element>
      )}
      {check.cron != null && (
        <Form.Element label="Cron">
          <Input
            autoFocus={false}
            maxLength={24}
            name="Cron"
            onChange={handleChangeName}
            placeholder="cron"
            size={ComponentSize.Small}
            spellCheck={false}
            testID="input-field"
            titleText="Title Text"
            type={InputType.Text}
            value={check.cron}
          />
        </Form.Element>
      )}
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    alerting: {check},
  } = getActiveTimeMachine(state)

  return {check}
}

const mdtp: DispatchProps = {
  updateTimeMachineCheck: updateTimeMachineCheck,
  changeCheckType: changeCheckType,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckMetaCard)
