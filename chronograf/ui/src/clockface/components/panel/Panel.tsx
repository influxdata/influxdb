// Libraries
import React, {Component, ComponentClass} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

// Components
import PanelHeader from 'src/reusable_ui/components/panel/PanelHeader'
import PanelBody from 'src/reusable_ui/components/panel/PanelBody'
import PanelFooter from 'src/reusable_ui/components/panel/PanelFooter'

import {ErrorHandling} from 'src/shared/decorators/errors'

export enum PanelType {
  Default = '',
  Solid = 'solid',
}

interface Props {
  children: JSX.Element[]
  type?: PanelType
}

@ErrorHandling
class Panel extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    type: PanelType.Default,
  }

  public static Header = PanelHeader
  public static Body = PanelBody
  public static Footer = PanelFooter

  public static ValidChildTypes: ComponentClass[] = [
    PanelHeader,
    PanelBody,
    PanelFooter,
  ]

  public static ValidChildNames: string = _.map(
    Panel.ValidChildTypes,
    child => {
      const name = child.displayName.split('Panel').pop()

      return `<Panel.${name}>`
    }
  ).join(', ')

  public render() {
    const {children} = this.props

    this.validateChildren()

    return <div className={this.className}>{children}</div>
  }

  private get className(): string {
    const {type} = this.props

    return classnames('panel', {'panel-solid': type === PanelType.Solid})
  }

  private validateChildren = (): void => {
    const childArray = React.Children.toArray(this.props.children)

    if (childArray.length === 0) {
      throw new Error(
        '<Panel> requires at least 1 child element. We recommend using <Panel.Body>'
      )
    }

    const childrenAreValid = _.every(childArray, this.childTypeIsValid)

    if (!childrenAreValid) {
      throw new Error(
        `<Panel> expected children of type ${Panel.ValidChildNames}`
      )
    }
  }

  private childTypeIsValid = (child: JSX.Element): boolean =>
    _.includes(Panel.ValidChildTypes, child.type)
}

export default Panel
