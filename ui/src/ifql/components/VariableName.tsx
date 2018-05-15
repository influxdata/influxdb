import React, {PureComponent} from 'react'

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
    return <div className="variable-string">{this.nameElement}</div>
  }

  private get nameElement(): JSX.Element {
    const {name} = this.props

    if (name.includes('=')) {
      return this.colorizeSyntax
    }

    return <span className="variable-name">{name}</span>
  }

  private get colorizeSyntax(): JSX.Element {
    const {name} = this.props
    const split = name.split('=')
    const varName = split[0].substring(0, split[0].length - 1)
    const varValue = this.props.name.replace(/^[^=]+=/, '')

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
