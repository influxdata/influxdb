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
} from '@influxdata/clockface'
import {Input} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import CheckTagRow from 'src/alerting/components/builder/CheckTagRow'
import DurationSelector from 'src/timeMachine/components/DurationSelector'

// Actions & Selectors
import {updateTimeMachineCheck} from 'src/timeMachine/actions'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {selectCheckEvery} from 'src/alerting/actions/checks'

// Constants
import {CHECK_EVERY_OPTIONS, CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'

// Types
import {Check, AppState, CheckTagSet} from 'src/types'

interface DispatchProps {
  onUpdateTimeMachineCheck: typeof updateTimeMachineCheck
  onSelectCheckEvery: typeof selectCheckEvery
}

interface StateProps {
  check: Partial<Check>
}

type Props = DispatchProps & StateProps

const CheckMetaCard: FC<Props> = ({
  check,
  onUpdateTimeMachineCheck,
  onSelectCheckEvery,
}) => {
  const handleChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>
  ) => {
    onUpdateTimeMachineCheck({[e.target.name]: e.target.value})
  }

  const addTagsRow = () => {
    const tags = check.tags || []
    onUpdateTimeMachineCheck({tags: [...tags, {key: '', value: ''}]})
  }

  const handleChangeTagRow = (index: number, tagSet: CheckTagSet) => {
    const tags = [...check.tags]
    tags[index] = tagSet
    onUpdateTimeMachineCheck({tags})
  }

  const handleRemoveTagRow = (index: number) => {
    let tags = [...check.tags]
    tags = tags.filter((_, i) => i !== index)
    onUpdateTimeMachineCheck({tags})
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
            <Form.Element label="Schedule Every">
              <DurationSelector
                selectedDuration={check.every}
                durations={CHECK_EVERY_OPTIONS}
                onSelectDuration={onSelectCheckEvery}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthSM={6}>
            <Form.Element label="Offset">
              <DurationSelector
                selectedDuration={check.offset}
                durations={CHECK_OFFSET_OPTIONS}
                onSelectDuration={offset => onUpdateTimeMachineCheck({offset})}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
      <Form.Label label="Tags" />
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
        text="+ Tag"
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
  onUpdateTimeMachineCheck: updateTimeMachineCheck,
  onSelectCheckEvery: selectCheckEvery,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckMetaCard)
