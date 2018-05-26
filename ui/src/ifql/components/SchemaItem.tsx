import React, {PureComponent, MouseEvent} from 'react'

interface Props {
  schemaType: string
  name: string
}

interface State {
  isOpen: boolean
}

export default class SchemaItem extends PureComponent<Props, State> {
  public render() {
    const {schemaType} = this.props
    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {name}
          <span className="ifql-schema-type">{schemaType}</span>
        </div>
      </div>
    )
  }

  private handleClick = (e: MouseEvent<HTMLDivElement>) => {
    e.stopPropagation()
    this.setState({isOpen: !this.state.isOpen})
  }

  private get className(): string {
    const {isOpen} = this.state
    const openClass = isOpen ? 'expanded' : ''

    return `ifql-schema-tree ifql-tree-node ${openClass}`
  }
}
