// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {Form, ComponentSize, ComponentColor, Grid} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import CheckTagRow from 'src/checks/components/CheckTagRow'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'
import DurationInput from 'src/shared/components/DurationInput'

// Actions
import {
  setOffset,
  removeTagSet,
  selectCheckEvery,
  editTagSetByIndex,
} from 'src/alerting/actions/alertBuilder'

// Constants
import {CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'
import {DURATIONS} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const EMPTY_TAG_SET = {
  key: '',
  value: '',
}

const CheckMetaCard: FC<Props> = ({
  tags,
  offset,
  every,
  onSelectCheckEvery,
  onSetOffset,
  onRemoveTagSet,
  onEditTagSetByIndex,
}) => {
  return (
    <BuilderCard
      testID="builder-meta"
      className="alert-builder--card alert-builder--meta-card"
    >
      <BuilderCard.Header title="Properties" />
      <BuilderCard.Body addPadding={true} autoHideScrollbars={true}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthSM={6}>
              <Form.Element label="Schedule Every">
                <DurationInput
                  value={every}
                  suggestions={DURATIONS}
                  onSubmit={onSelectCheckEvery}
                  testID="schedule-check"
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={6}>
              <Form.Element label="Offset">
                <DurationInput
                  value={offset}
                  suggestions={CHECK_OFFSET_OPTIONS}
                  onSubmit={onSetOffset}
                  testID="offset-options"
                />
              </Form.Element>
            </Grid.Column>
          </Grid.Row>
        </Grid>
        <Form.Label label="Tags" />
        {tags.map((t, i) => (
          <CheckTagRow
            key={i}
            index={i}
            tagSet={t}
            handleChangeTagRow={onEditTagSetByIndex}
            handleRemoveTagRow={onRemoveTagSet}
          />
        ))}
        <DashedButton
          text="+ Tag"
          onClick={() => onEditTagSetByIndex(tags.length, EMPTY_TAG_SET)}
          color={ComponentColor.Primary}
          size={ComponentSize.Small}
        />
      </BuilderCard.Body>
    </BuilderCard>
  )
}

const mstp = ({alertBuilder: {tags, offset, every}}: AppState) => {
  return {
    tags: tags || [],
    offset,
    every,
  }
}

const mdtp = {
  onSelectCheckEvery: selectCheckEvery,
  onSetOffset: setOffset,
  onRemoveTagSet: removeTagSet,
  onEditTagSetByIndex: editTagSetByIndex,
}

const connector = connect(mstp, mdtp)

export default connector(CheckMetaCard)
