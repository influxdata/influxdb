// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, ComponentSize, ComponentColor, Grid} from '@influxdata/clockface'
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import CheckTagRow from 'src/alerting/components/builder/CheckTagRow'
import DurationSelector from 'src/shared/components/DurationSelector'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

// Actions
import {
  setOffset,
  removeTagSet,
  selectCheckEvery,
  editTagSetByIndex,
} from 'src/alerting/actions/alertBuilder'

// Constants
import {CHECK_EVERY_OPTIONS, CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'

// Types
import {AppState, CheckTagSet} from 'src/types'

interface DispatchProps {
  onSelectCheckEvery: typeof selectCheckEvery
  onSetOffset: typeof setOffset
  onRemoveTagSet: typeof removeTagSet
  onEditTagSetByIndex: typeof editTagSetByIndex
}

interface StateProps {
  tags: CheckTagSet[]
  offset: string
  every: string
}

type Props = DispatchProps & StateProps

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
                <DurationSelector
                  selectedDuration={every}
                  durations={CHECK_EVERY_OPTIONS}
                  onSelectDuration={onSelectCheckEvery}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={6}>
              <Form.Element label="Offset">
                <DurationSelector
                  selectedDuration={offset}
                  durations={CHECK_OFFSET_OPTIONS}
                  onSelectDuration={onSetOffset}
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

const mstp = ({alertBuilder: {tags, offset, every}}: AppState): StateProps => {
  return {tags, offset, every}
}

const mdtp: DispatchProps = {
  onSelectCheckEvery: selectCheckEvery,
  onSetOffset: setOffset,
  onRemoveTagSet: removeTagSet,
  onEditTagSetByIndex: editTagSetByIndex,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckMetaCard)
