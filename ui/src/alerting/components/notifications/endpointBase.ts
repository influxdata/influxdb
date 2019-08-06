import {SlackBase, SMTPBase, PagerDutyBase} from 'src/types'

type EndpointBase = SlackBase | SMTPBase | PagerDutyBase

export const getEndpointBase = (endpoints, id): EndpointBase => {
  const endpoint = endpoints.find(e => e.id === id)

  switch (endpoint.type) {
    case 'slack': {
      return {messageTemplate: '', channel: '', type: 'slack'}
    }
    case 'smtp': {
      return {to: '', bodyTemplate: '', subjectTemplate: '', type: 'smtp'}
    }
    case 'pagerduty': {
      return {messageTemplate: '', type: 'pagerduty'}
    }

    default: {
      throw new Error('Unknown endpoint type in <RuleMessage />')
    }
  }
}
