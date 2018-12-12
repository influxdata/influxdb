// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import LabelTooltip from 'src/clockface/components/label/LabelTooltip'

interface Props {
  children: JSX.Element | JSX.Element[]
  className?: string
  limitChildCount?: number
}

class LabelContainer extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    limitChildCount: 999,
  }

  public render() {
    const {className} = this.props

    return (
      <div
        className={classnames('label--container', {
          [`${className}`]: className,
        })}
      >
        <div className="label--container-margin">
          {this.children}
          {this.additionalChildrenIndicator}
        </div>
      </div>
    )
  }

  private get children(): JSX.Element[] {
    const {children, limitChildCount} = this.props

    return React.Children.map(children, (child: JSX.Element, i: number) => {
      if (i < limitChildCount) {
        return child
      }
    })
  }

  private get additionalChildrenIndicator(): JSX.Element {
    const {children, limitChildCount} = this.props

    const childCount = React.Children.count(children)

    if (limitChildCount < childCount) {
      const additionalCount = childCount - limitChildCount
      return (
        <div className="additional-labels">
          +{additionalCount} more
          <LabelTooltip labels={children} />
        </div>
      )
    }
  }
}
export default LabelContainer
