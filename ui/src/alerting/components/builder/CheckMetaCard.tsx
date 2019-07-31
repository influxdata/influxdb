// Libraries
import React, {FC} from 'react'
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
import {updateCurrentCheck} from 'src/alerting/actions/checks'

// Types
import {Check, AppState} from 'src/types'
import {
  DEFAULT_CHECK_EVERY,
  DEFAULT_CHECK_OFFSET,
  DEFAULT_CHECK_CRON,
} from 'src/alerting/constants'

interface DispatchProps {
  updateCurrentCheck: typeof updateCurrentCheck
}

interface StateProps {
  check: Partial<Check>
}

type Props = DispatchProps & StateProps

const CheckMetaCard: FC<Props> = ({updateCurrentCheck, check}) => {
  const handleChangeType = (type: typeof check.type) => {
    updateCurrentCheck({type})
  }

  const handleChangeName = (e: React.ChangeEvent<HTMLInputElement>) => {
    updateCurrentCheck({name: e.target.value})
  }

  const handleChangeMessage = (statusMessageTemplate: string) => {
    updateCurrentCheck({statusMessageTemplate})
  }

  const handleChangeSchedule = (scheduleType: 'cron' | 'every') => {
    if (scheduleType == 'cron' && !check.cron) {
      updateCurrentCheck({cron: DEFAULT_CHECK_CRON, every: null, offset: null})
      return
    }
    if (scheduleType == 'every' && !check.every) {
      updateCurrentCheck({
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
      <Form.Element label="Status Message Template">
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
          value={check.name}
        />
      </Form.Element>
      <Form.Element label="Offset">
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
          value={check.name}
        />
      </Form.Element>
      <Form.Element label="Range">
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
          value={check.name}
        />
      </Form.Element>
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    checks: {
      current: {check},
    },
  } = state

  return {check}
}

const mdtp: DispatchProps = {
  updateCurrentCheck: updateCurrentCheck,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckMetaCard)
