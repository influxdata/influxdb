import React, {PureComponent, MouseEvent} from 'react'

interface Props {
  measurement: string
}

interface State {
  isOpen: boolean
}

export default class MeasurementListItem extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isOpen: false,
    }
  }

  public render() {
    const {measurement} = this.props

    return (
      <div className={this.className}>
        <div className="ifql-schema-item" onClick={this.handleClick}>
          <div className="ifql-schema-item-toggle" />
          {measurement}
          <span className="ifql-schema-type">Measurement</span>
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
