import React, {PureComponent, ReactElement} from 'react'
import ReactDOM from 'react-dom'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: ReactElement<any>
  onClickOutside: (e) => void
}

@ErrorHandling
export class ClickOutside extends PureComponent<Props> {
  public componentDidMount() {
    document.addEventListener('click', this.handleClickOutside, true)
  }

  public componentWillUnmount() {
    document.removeEventListener('click', this.handleClickOutside, true)
  }

  public render() {
    return React.Children.only(this.props.children)
  }

  private handleClickOutside = e => {
    const domNode = ReactDOM.findDOMNode(this)
    if (!domNode || !domNode.contains(e.target)) {
      this.props.onClickOutside(e)
    }
  }
}
