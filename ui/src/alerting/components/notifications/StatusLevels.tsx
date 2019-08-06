// Libraries
import React, {FC, useContext} from 'react'

// Components
import {
  ComponentSpacer,
  TextBlock,
  FlexDirection,
  ComponentSize,
} from '@influxdata/clockface'
import LevelsDropdown from 'src/alerting/components/notifications/LevelsDropdown'
import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'
import {NewRuleDispatch} from './NewRuleOverlay'

// Types
import {StatusRuleItem} from 'src/types'

interface Props {
  status: StatusRuleItem
}

const StatusLevels: FC<Props> = ({status}) => {
  const {currentLevel, previousLevel} = status.value
  const dispatch = useContext(NewRuleDispatch)

  const onClickLevel = (type, level) => {
    const value = {
      ...status.value,
      [type]: {
        ...status.value[type],
        level,
      },
    }

    dispatch({
      type: 'UPDATE_STATUS_RULES',
      statusRule: {
        ...status,
        value,
      },
    })
  }

  return (
    <ComponentSpacer direction={FlexDirection.Row} margin={ComponentSize.Small}>
      <TextBlock text="When status" />
      <ComponentSpacer.FlexChild grow={0} basis={140}>
        <StatusChangeDropdown status={status} />
      </ComponentSpacer.FlexChild>
      {!!previousLevel && (
        <ComponentSpacer.FlexChild grow={0} basis={140}>
          <LevelsDropdown
            type="previousLevel"
            selectedLevel={previousLevel.level}
            onClickLevel={onClickLevel}
          />
        </ComponentSpacer.FlexChild>
      )}
      {!!previousLevel && <TextBlock text="to" />}
      <ComponentSpacer.FlexChild grow={0} basis={140}>
        <LevelsDropdown
          type="currentLevel"
          selectedLevel={currentLevel.level}
          onClickLevel={onClickLevel}
        />
      </ComponentSpacer.FlexChild>
    </ComponentSpacer>
  )
}

export default StatusLevels
