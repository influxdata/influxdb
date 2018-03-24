import React, {PureComponent} from 'react'

interface State {
  isOpen: boolean
}

interface Props {
  funcs: string[]
}

class FuncsButton extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      isOpen: false,
    }
  }

  public render() {
    const {isOpen} = this.state
    const {funcs} = this.props

    return (
      <div className={`dropdown dashboard-switcher ${isOpen ? 'open' : ''}`}>
        <button
          className="btn btn-square btn-default btn-sm dropdown-toggle"
          onClick={this.handleClick}
        >
          <span className="icon plus" />
        </button>
        <ul className="dropdown-menu">
          {isOpen &&
            funcs.map((func, i) => (
              <li className="dropdown-item func" data-test="func-item" key={i}>
                <a>{func}</a>
              </li>
            ))}
        </ul>
      </div>
    )
  }

  private handleClick = () => {
    this.setState({isOpen: !this.state.isOpen})
  }
}

export default FuncsButton
