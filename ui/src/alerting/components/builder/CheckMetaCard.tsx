// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Radio,
  ButtonShape,
  Form,
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
          readOnly={false}
          required={false}
          size={ComponentSize.Medium}
          spellCheck={false}
          testID="status-message-textarea"
          value={check.statusMessageTemplate}
          wrap={Wrap.Soft}
        />
      </Form.Element>
      <Grid>
        <Grid.Row>
          <Grid.Column widthSM={6}>
            <Form.Element label="Run this check every">
              <Input
                name="every"
                onChange={handleChange}
                titleText="Check run interval"
                value={check.every}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthSM={6}>
            <Form.Element label="Offset">
              <Input
                name="offset"
                onChange={handleChange}
                titleText="Offset check interval"
                value={check.offset}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
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
