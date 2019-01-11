// Libraries
import React, {Component, ComponentClass} from 'react'
import _ from 'lodash'

// Components
import PanelHeader from 'src/clockface/components/panel/PanelHeader'
import PanelBody from 'src/clockface/components/panel/PanelBody'
import PanelFooter from 'src/clockface/components/panel/PanelFooter'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
}

@ErrorHandling
class Panel extends Component<Props> {
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

    return <div className="panel">{children}</div>
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
