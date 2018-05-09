import React, {PureComponent, MouseEvent} from 'react'

interface Props {
  name?: string
}

interface State {
  isExpanded: boolean
}

export default class VariableName extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  constructor(props) {
    super(props)

    this.state = {
      isExpanded: false,
    }
  }

  public render() {
    const {isExpanded} = this.state

    return (
      <div
        className="variable-string"
        onMouseEnter={this.handleMouseEnter}
        onMouseLeave={this.handleMouseLeave}
      >
        {this.nameElement}
        {isExpanded && this.renderTooltip}
      </div>
    )
  }

  private get renderTooltip(): JSX.Element {
    const {name} = this.props

    if (name.includes('=')) {
      const split = name.split('=')
      const varName = split[0].substring(0, split[0].length - 1)
      const varValue = split[1].substring(1)

      return (
        <div className="variable-name--tooltip">
          <input
            type="text"
            className="form-control form-plutonium input-sm variable-name--input"
            defaultValue={varName}
            placeholder="Name"
          />
          <span className="variable-name--operator">=</span>
          <input
            type="text"
            className="form-control input-sm variable-name--input"
            defaultValue={varValue}
            placeholder="Value"
          />
        </div>
      )
    }

    return (
      <div className="variable-name--tooltip">
        <input
          type="text"
          className="form-control form-plutonium input-sm variable-name--input"
          defaultValue={name}
          placeholder="Name this query..."
        />
      </div>
    )
  }

  private handleMouseEnter = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: true})
  }

  private handleMouseLeave = (e: MouseEvent<HTMLElement>): void => {
    e.stopPropagation()

    this.setState({isExpanded: false})
  }

  private get nameElement(): JSX.Element {
    const {name} = this.props

    if (!name) {
      return <span className="variable-blank">Untitled</span>
    }

    if (name.includes('=')) {
      return this.colorizeSyntax
    }

    return <span className="variable-name">{name}</span>
  }

  private get colorizeSyntax(): JSX.Element {
    const {name} = this.props
    const split = name.split('=')
    const varName = split[0].substring(0, split[0].length - 1)
    const varValue = split[1].substring(1)

    const valueIsString = varValue.endsWith('"')

    return (
      <>
        <span className="variable-name">{varName}</span>
        {' = '}
        <span
          className={
            valueIsString ? 'variable-value--string' : 'variable-value--number'
          }
        >
          {varValue}
        </span>
      </>
    )
  }
}
