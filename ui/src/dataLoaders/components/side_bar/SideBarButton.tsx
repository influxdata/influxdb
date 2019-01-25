// Libraries
import React, {Component} from 'react'

// Components
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

// Types
import {IconFont} from 'src/clockface'

interface Props {
  text: string
  titleText: string
  color: ComponentColor
  icon?: IconFont
  onClick?: () => void
}

class SideBarButton extends Component<Props> {
  public render() {
    const {text, titleText, color, onClick, icon} = this.props

    return (
      <Button
        customClass="side-bar--button"
        text={text}
        titleText={titleText}
        onClick={onClick}
        size={ComponentSize.Small}
        color={color}
        icon={icon}
      />
    )
  }
}

export default SideBarButton
