import React, {PureComponent} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

export type OnToggleView = (view: string) => void

interface TabProps {
  view: string
  currentView: string
  onToggleView: OnToggleView
}

class VisHeaderTab extends PureComponent<TabProps> {
  public render() {
    return (
      <li className={this.className} onClick={this.handleClick}>
        {this.text}
      </li>
    )
  }

  private get className(): string {
    const {view, currentView} = this.props
    return classnames({active: view === currentView})
  }

  private handleClick = () => {
    this.props.onToggleView(this.props.view)
  }

  private get text(): string {
    return _.upperFirst(this.props.view)
  }
}

export default VisHeaderTab
