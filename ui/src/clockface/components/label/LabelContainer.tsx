// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import LabelTooltip from 'src/clockface/components/label/LabelTooltip'

// Styles
import 'src/clockface/components/label/LabelContainer.scss'

interface PassedProps {
  renderEditButton: () => JSX.Element
  children?: JSX.Element[]
  className?: string
}

interface DefaultProps {
  limitChildCount?: number
  resourceName?: string
}

type Props = PassedProps & DefaultProps

class LabelContainer extends Component<Props> {
  public static defaultProps: DefaultProps = {
    limitChildCount: 999,
    resourceName: 'this resource',
  }

  public render() {
    const {className, renderEditButton} = this.props

    return (
      <div
        className={classnames('label--container', {
          [`${className}`]: className,
        })}
      >
        <div className="label--container-margin">
          {this.children}
          {this.additionalChildrenIndicator}
          {renderEditButton()}
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
}
export default LabelContainer
