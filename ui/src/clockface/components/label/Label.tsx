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

export interface LabelType {
  id: string
  name: string
  description: string
  colorHex: string
  onClick?: (id: string) => void
  onDelete?: (id: string) => void
}

interface LabelProps {
  size?: ComponentSize
}

interface State {
  isMouseOver: boolean
}

type Props = LabelType & LabelProps

@ErrorHandling
class Label extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    size: ComponentSize.ExtraSmall,
  }

  public static Container = LabelContainer

  constructor(props: Props) {
    super(props)

    this.state = {
      isMouseOver: false,
    }
  }

  public render() {
    const {name} = this.props

    this.validateColorHex()

    return (
      <div
        className={this.className}
        onClick={this.handleClick}
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
        style={this.style}
        title={this.title}
      >
        <label>{name}</label>
        {this.deleteButton}
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>): void => {
    e.preventDefault()
    const {id, onClick} = this.props

    if (onClick) {
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
      'label--clickable': onClick,
      'label--deletable': onDelete,
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
    const {onDelete, name} = this.props

    if (onDelete) {
      return (
        <button
          className="label--delete"
          onClick={this.handleDelete}
          type="button"
          title={`Click × to remove "${name}"`}
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
