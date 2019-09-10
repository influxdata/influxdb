// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  TextBlock,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import RuleLevelsDropdown from 'src/alerting/components/notifications/RuleLevelsDropdown'
// import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'
import {LevelType} from 'src/alerting/components/notifications/RuleOverlay.reducer'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'

// Types
import {StatusRuleDraft, RuleStatusLevel} from 'src/types'

interface Props {
  status: StatusRuleDraft
}

const StatusLevels: FC<Props> = ({status}) => {
  const {currentLevel, previousLevel} = status.value
  const dispatch = useRuleDispatch()

  const onClickLevel = (levelType: LevelType, level: RuleStatusLevel) => {
    dispatch({
      type: 'UPDATE_STATUS_LEVEL',
      statusID: status.cid,
      levelType,
      level,
    })
  }

  // TODO: remove these comments when status change actually works
  return (
    <FlexBox direction={FlexDirection.Row} margin={ComponentSize.Small}>
      <TextBlock text="When status" />
      <FlexBox.Child grow={0} basis={140}>
        <TextBlock text="is equal to" />
        {/*<StatusChangeDropdown status={status} />*/}
      </FlexBox.Child>
      {/*{!!previousLevel && (
        <FlexBox.Child grow={0} basis={140}>
          <RuleLevelsDropdown
            type="previousLevel"
            selectedLevel={previousLevel}
            otherLevel={currentLevel}
            onClickLevel={onClickLevel}
          />
        </FlexBox.Child>
      )*/}
      {/* !!previousLevel && <TextBlock text="to" /> */}
      <FlexBox.Child grow={0} basis={140}>
        <RuleLevelsDropdown
          type="currentLevel"
          selectedLevel={currentLevel}
          otherLevel={previousLevel}
          onClickLevel={onClickLevel}
        />
      </FlexBox.Child>
    </FlexBox>
  )
}

export default StatusLevels
