// Libraries
import React, {Component, CSSProperties, MouseEvent} from 'react'
import chroma from 'chroma-js'
import classnames from 'classnames'

// Types
import {ComponentSize, Greys} from 'src/clockface/types'

// Components
import LabelContainer from 'src/clockface/components/label/LabelContainer'

// Styles
import './Label.scss'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedProps {
  id: string
  name: string
  description: string
  colorHex: string
  onClick?: (id: string) => void
  onDelete?: (id: string) => void
}

interface DefaultProps {
  size?: ComponentSize
  testID?: string
}

interface State {
  isMouseOver: boolean
}

type Props = PassedProps & DefaultProps

@ErrorHandling
class Label extends Component<Props, State> {
  public static defaultProps: DefaultProps = {
    size: ComponentSize.ExtraSmall,
    testID: 'label--pill',
  }

  public static Container = LabelContainer

  constructor(props: Props) {
    super(props)

    this.state = {
      isMouseOver: false,
    }
  }

  public render() {
    const {name, testID} = this.props

    this.validateColorHex()

    return (
      <div
        className={this.className}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        style={this.style}
        title={this.title}
      >
        <span
          className="label--name"
          onClick={this.handleClick}
          data-testid={`${testID} ${name}`}
        >
          {name}
        </span>
        {this.deleteButton}
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    const {id, onClick} = this.props

    if (onClick) {
      e.stopPropagation()
      e.preventDefault()
      onClick(id)
    }
  }

  private handleDelete = (): void => {
    const {id, onDelete} = this.props

    if (onDelete) {
      onDelete(id)
    }
  }

  private handleMouseEnter = (): void => {
    const {onClick} = this.props

    if (onClick) {
      this.setState({isMouseOver: true})
    }
  }

  private handleMouseLeave = (): void => {
    const {onClick} = this.props

    if (onClick) {
      this.setState({isMouseOver: false})
    }
  }

  private get className(): string {
    const {size, onClick, onDelete} = this.props

    return classnames('label', {
      [`label--${size}`]: size,
      'label--deletable': onDelete,
      'label--clickable': onClick,
    })
  }

  private get title(): string {
    const {onClick, name, description} = this.props

    if (onClick) {
      return `Click to see all resources with the "${name}" label`
    }

    return `${description}`
  }

  private get deleteButton(): JSX.Element {
    const {onDelete, name, testID} = this.props

    if (onDelete) {
      return (
        <button
          className="label--delete"
          onClick={this.handleDelete}
          type="button"
          title={`Remove label "${name}"`}
          data-testid={`${testID}--delete ${name}`}
        >
          <div
            className="label--delete-x"
            style={{backgroundColor: this.textColor}}
          />
          <div
            className="label--delete-x"
            style={{backgroundColor: this.textColor}}
          />
        </button>
      )
    }
  }

  private get style(): CSSProperties {
    const {isMouseOver} = this.state
    const {colorHex, onClick} = this.props

    let backgroundColor = colorHex

    if (isMouseOver && onClick) {
      backgroundColor = `${chroma(colorHex).brighten(1)}`
    }

    return {
      backgroundColor: `${backgroundColor}`,
      color: this.textColor,
    }
  }

  private get textColor(): string {
    const {colorHex} = this.props

    const darkContrast = chroma.contrast(colorHex, Greys.Kevlar)
    const lightContrast = chroma.contrast(colorHex, Greys.White)

    if (darkContrast > lightContrast) {
      return Greys.Kevlar
    } else {
      return Greys.White
    }
  }

  private validateColorHex = (): void => {
    const {colorHex} = this.props

    const isValidLength = colorHex.length === 7
    const containsValidCharacters =
      colorHex.replace(/[ABCDEF0abcdef123456789]+/g, '') === '#'

    if (!isValidLength || !containsValidCharacters) {
      throw new Error(
        '<Label /> component has been passed a invalid colorHex prop'
      )
    }
  }
}

export default Label
