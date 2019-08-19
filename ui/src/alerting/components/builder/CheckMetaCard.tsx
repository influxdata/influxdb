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
  ComponentColor,
  Grid,
} from '@influxdata/clockface'
import {Input} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import CheckTagRow from 'src/alerting/components/builder/CheckTagRow'

// Actions
import {updateTimeMachineCheck, changeCheckType} from 'src/timeMachine/actions'

//Selectors
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Check, AppState, CheckType, CheckTagSet} from 'src/types'
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

  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    updateTimeMachineCheck({[e.target.name]: e.target.value})
  }

  const addTagsRow = () => {
    const tags = check.tags || []
    updateTimeMachineCheck({tags: [...tags, {key: '', value: ''}]})
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
  const handleChangeTagRow = (index: number, tagSet: CheckTagSet) => {
    const tags = [...check.tags]
    tags[index] = tagSet
    updateTimeMachineCheck({tags})
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
          name="name"
          onChange={handleChange}
          placeholder="Name this check"
          size={ComponentSize.Small}
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
          name="statusMessageTemplate"
          onChange={handleChange}
          placeholder="Example: {tags.cpu} exceeded threshold: {value}%"
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
      <Grid>
        <Grid.Row>
          <Grid.Column widthSM={6}>
            {check.every != null && (
              <Form.Element label="Every">
                <Input
                  name="every"
                  onChange={handleChange}
                  titleText="Name of the check"
                  value={check.every}
                />
              </Form.Element>
            )}
          </Grid.Column>
          <Grid.Column widthSM={6}>
            {check.offset != null && (
              <Form.Element label="Offset">
                <Input
                  name="offset"
                  onChange={handleChange}
                  titleText="Offset check interval"
                  value={check.offset}
                />
              </Form.Element>
            )}
          </Grid.Column>
        </Grid.Row>
      </Grid>
      {check.cron != null && (
        <Form.Element label="Cron">
          <Input
            autoFocus={false}
            maxLength={24}
            name="cron"
            onChange={handleChange}
            placeholder="cron"
            size={ComponentSize.Small}
            spellCheck={false}
            testID="input-field"
            titleText="Use cron format to specify interval"
            type={InputType.Text}
            value={check.cron}
          />
        </Form.Element>
      )}
      {check.tags &&
        check.tags.map((t, i) => (
          <CheckTagRow
            key={i}
            index={i}
            tagSet={t}
            handleChangeTagRow={handleChangeTagRow}
          />
        ))}
      <DashedButton
        text="+ Tags"
        onClick={addTagsRow}
        color={ComponentColor.Primary}
        size={ComponentSize.Small}
      />
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
