// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import LabelTooltip from 'src/clockface/components/label/LabelTooltip'
import {Button} from '@influxdata/clockface'

// Types
import {ButtonShape, IconFont, ComponentColor} from '@influxdata/clockface'

// Styles
import 'src/clockface/components/label/LabelContainer.scss'

interface Props {
  children?: JSX.Element[]
  className?: string
  limitChildCount?: number
  resourceName?: string
  onEdit?: () => void
  testID?: string
}

class LabelContainer extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    limitChildCount: 999,
    resourceName: 'this resource',
    testID: 'labels-con',
  }

  public render() {
    const {className, testID} = this.props

    return (
      <div
        className={classnames('label--container', {
          [`${className}`]: className,
        })}
        data-testid={testID}
      >
        <div className="label--container-margin">
          {this.children}
          {this.additionalChildrenIndicator}
          {this.editButton}
        </div>
      </div>
    )
  }

  private get sortedChildren(): JSX.Element[] {
    const {children} = this.props

    if (children && React.Children.count(children) > 1) {
      return children.sort((a: JSX.Element, b: JSX.Element) => {
        const textA = a.props.name.toUpperCase()
        const textB = b.props.name.toUpperCase()
        return textA < textB ? -1 : textA > textB ? 1 : 0
      })
    }

    return children
  }

  private get children(): JSX.Element[] | JSX.Element {
    const {children, limitChildCount} = this.props

    if (children) {
      return React.Children.map(
        this.sortedChildren,
        (child: JSX.Element, i: number) => {
          if (i < limitChildCount) {
            return child
          }
        }
      )
    }
  }

  private get additionalChildrenIndicator(): JSX.Element {
    const {children, limitChildCount} = this.props

    const childCount = React.Children.count(children)

    if (limitChildCount < childCount) {
      const additionalCount = childCount - limitChildCount
      return (
        <div className="additional-labels">
          +{additionalCount} more
          <LabelTooltip labels={this.sortedChildren.slice(limitChildCount)} />
        </div>
      )
    }
  }

  private get editButton(): JSX.Element {
    const {onEdit, children, resourceName} = this.props

    const titleText = React.Children.count(children)
      ? `Edit Labels for ${resourceName}`
      : `Add Labels to ${resourceName}`

    if (onEdit) {
      return (
        <div className="label--edit-button">
          <Button
            color={ComponentColor.Primary}
            titleText={titleText}
            onClick={onEdit}
            shape={ButtonShape.Square}
            icon={IconFont.Plus}
          />
        </div>
      )
    }
  }
}
export default LabelContainer
