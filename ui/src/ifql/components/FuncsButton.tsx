import React, {PureComponent} from 'react'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import OnClickOutside from 'src/shared/components/OnClickOutside'

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
        <ul className="dropdown-menu funcs">
          <FancyScrollbar autoHide={false} autoHeight={true} maxHeight={240}>
            {isOpen &&
              funcs.map((func, i) => (
                <li
                  className="dropdown-item func"
                  data-test="func-item"
                  key={i}
                >
                  <a>{func}</a>
                </li>
              ))}
          </FancyScrollbar>
        </ul>
      </div>
    )
  }

  private handleClick = () => {
    this.setState({isOpen: !this.state.isOpen})
  }

  // tslint:disable-next-line
  private handleClickOutside = () => {
    this.setState({isOpen: false})
  }
}

export default OnClickOutside(FuncsButton)
