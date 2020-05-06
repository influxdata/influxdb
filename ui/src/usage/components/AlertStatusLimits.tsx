// Libraries
import React, {Component} from 'react'
import {capitalize} from 'lodash'
import {
  Alert,
  ComponentColor,
  IconFont,
  FlexBox,
  JustifyContent,
  FlexDirection,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'

// Components
import ConversionButton from './ConversionButton'

// Types
import {UsageStatusObj} from 'src/types'

interface Props {
  statuses: UsageStatusObj[]
  isOperator: boolean
  accountType: string
}

class AlertStatusLimits extends Component<Props> {
  render() {
    const {statuses} = this.props
    const exceeded = statuses.filter(s => s.status === 'exceeded')

    if (exceeded.length === 0) {
      return null
    }

    return (
      <Alert color={ComponentColor.Primary} icon={IconFont.AlertTriangle}>
        {this.formattedMessage(exceeded)}
      </Alert>
    )
  }

  formattedMessage(exceeded: UsageStatusObj[]) {
    const {isOperator, accountType} = this.props

    const message = this.messageText(exceeded)

    if (isOperator) {
      return <p>{message}</p>
    }

    const isFreeAccount = accountType == 'free'

    return (
      <FlexBox
        alignItems={AlignItems.Center}
        direction={FlexDirection.Row}
        stretchToFitWidth={true}
        justifyContent={JustifyContent.SpaceBetween}
        margin={ComponentSize.Small}
      >
        <div>{message}</div>
        {isFreeAccount ? <ConversionButton /> : null}
      </FlexBox>
    )
  }

  messageText(exceeded: UsageStatusObj[]) {
    const {isOperator} = this.props

    const limitNames = this.limitNames(exceeded)

    if (isOperator) {
      return `${capitalize(limitNames)} limits have been exceeded.`
    }

    return `Hey there, it looks like you have exceeded your plan's ${limitNames} limits.${this.additionalMessage(
      exceeded
    )}`
  }

  limitNames(exceeded: UsageStatusObj[]) {
    const {isOperator} = this.props

    const renamedLimits = exceeded.map(limit => {
      if (limit.name === 'cardinality' && !isOperator) {
        return 'total series'
      }

      return limit.name
    })

    return renamedLimits.join(' and ')
  }

  additionalMessage(exceeded) {
    if (exceeded.includes('cardinality')) {
      return ' Your writes will be rejected until resolved.'
    }

    return ''
  }
}

export default AlertStatusLimits
