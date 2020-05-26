// Libraries
import React, {FC} from 'react'

// Components
import SlackMessage from './SlackMessage'
import SMTPMessage from './SMTPMessage'
import PagerDutyMessage from './PagerDutyMessage'
import TelegramMessage from './TelegramMessage'

// Utils
import {useRuleDispatch} from './RuleOverlayProvider'

// Types
import {NotificationRuleDraft} from 'src/types'

interface Props {
  rule: NotificationRuleDraft
}

const RuleMessageContents: FC<Props> = ({rule}) => {
  const dispatch = useRuleDispatch()
  const onChange = ({target}) => {
    const {name, value} = target

    dispatch({
      type: 'UPDATE_RULE',
      rule: {
        ...rule,
        [name]: value,
      },
    })
  }

  switch (rule.type) {
    case 'slack': {
      const {messageTemplate, channel} = rule
      return (
        <SlackMessage
          messageTemplate={messageTemplate}
          channel={channel}
          onChange={onChange}
        />
      )
    }
    case 'smtp': {
      const {to, subjectTemplate, bodyTemplate} = rule
      return (
        <SMTPMessage
          to={to}
          onChange={onChange}
          bodyTemplate={bodyTemplate}
          subjectTemplate={subjectTemplate}
        />
      )
    }
    case 'pagerduty': {
      const {messageTemplate} = rule
      return (
        <PagerDutyMessage
          messageTemplate={messageTemplate}
          onChange={onChange}
        />
      )
    }
    case 'telegram': {
      const {messageTemplate, channel} = rule
      return (
        <TelegramMessage
          messageTemplate={messageTemplate}
          channel={channel}
          onChange={onChange}
        />
      )
    }
    case 'http': {
      return <></>
    }

    default:
      throw new Error('Unexpected endpoint type in <RuleMessageContents/>.')
  }
}

export default RuleMessageContents
