// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Form,
  ComponentSize,
  TextArea,
  AutoComplete,
  Wrap,
  ComponentColor,
  Grid,
  InfluxColors,
} from '@influxdata/clockface'
import {Input} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import CheckTagRow from 'src/alerting/components/builder/CheckTagRow'

// Actions & Selectors
import {updateTimeMachineCheck} from 'src/timeMachine/actions'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Check, AppState, CheckTagSet} from 'src/types'

interface DispatchProps {
  updateTimeMachineCheck: typeof updateTimeMachineCheck
}

interface StateProps {
  check: Partial<Check>
}

type Props = DispatchProps & StateProps

const CheckMetaCard: FC<Props> = ({updateTimeMachineCheck, check}) => {
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

  const handleRemoveTagRow = (index: number) => {
    let tags = [...check.tags]
    tags = tags.filter((_, i) => i !== index)
    updateTimeMachineCheck({tags})
  }

  return (
    <>
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
            <Form.Element label="Schedule every">
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
                titleText="Check offset interval"
                value={check.offset}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <Form.Label label="Tags :" />
      <Form.Divider lineColor={InfluxColors.Smoke} />
      {check.tags &&
        check.tags.map((t, i) => (
          <CheckTagRow
            key={i}
            index={i}
            tagSet={t}
            handleChangeTagRow={handleChangeTagRow}
            handleRemoveTagRow={handleRemoveTagRow}
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
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckMetaCard)
