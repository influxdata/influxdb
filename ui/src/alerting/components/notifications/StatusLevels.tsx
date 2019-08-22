// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  TextBlock,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import LevelsDropdown from 'src/alerting/components/notifications/LevelsDropdown'
import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'
import {LevelType} from 'src/alerting/components/notifications/RuleOverlay.reducer'

// Utils
import {useRuleDispatch} from './RuleOverlay.reducer'

// Types
import {StatusRuleDraft, CheckStatusLevel} from 'src/types'

interface Props {
  status: StatusRuleDraft
}

const StatusLevels: FC<Props> = ({status}) => {
  const {currentLevel, previousLevel} = status.value
  const dispatch = useRuleDispatch()

  const onClickLevel = (levelType: LevelType, level: CheckStatusLevel) => {
    dispatch({
      type: 'UPDATE_STATUS_LEVEL',
      statusID: status.cid,
      levelType,
      level,
    })
  }

  return (
    <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
      <TextBlock text="When status" />
      <FlexBox.Child grow={0} basis={140}>
        <StatusChangeDropdown status={status} />
      </FlexBox.Child>
      {!!previousLevel && (
        <FlexBox.Child grow={0} basis={140}>
          <LevelsDropdown
            type="previousLevel"
            selectedLevel={previousLevel.level}
            onClickLevel={onClickLevel}
          />
        </FlexBox.Child>
      )}
      {!!previousLevel && <TextBlock text="to" />}
      <FlexBox.Child grow={0} basis={140}>
        <LevelsDropdown
          type="currentLevel"
          selectedLevel={currentLevel.level}
          onClickLevel={onClickLevel}
        />
      </FlexBox.Child>
    </FlexBox>
  )
}

export default StatusLevels
