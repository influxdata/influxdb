import React, {PureComponent} from 'react'
import uuid from 'uuid'

import {ClickOutside} from 'src/shared/components/ClickOutside'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  items: Item[]
  onChoose: (item: Item) => void
}

interface Item {
  text?: string
  name?: string
}

interface State {
  open: boolean
}

@ErrorHandling
class TagsAddButton extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {open: false}
  }

  public render() {
    const {open} = this.state
    const {items} = this.props

    const classname = `tags-add${open ? ' open' : ''}`
    return (
      <ClickOutside onClickOutside={this.handleClickOutside}>
        <div className={classname} onClick={this.handleButtonClick}>
          <span className="icon plus" />
          <div className="tags-add--menu">
            {items.map(item => (
              <div
                key={uuid.v4()}
                className="tags-add--menu-item"
                onClick={this.handleMenuClick(item)}
              >
                {item.text}
              </div>
            ))}
          </div>
        </div>
      </ClickOutside>
    )
  }

  private handleButtonClick = () => {
    this.setState({open: !this.state.open})
  }

  private handleMenuClick = item => () => {
    this.setState({open: false})
    this.props.onChoose(item)
  }

  private handleClickOutside = () => {
    this.setState({open: false})
  }
}

export default TagsAddButton
