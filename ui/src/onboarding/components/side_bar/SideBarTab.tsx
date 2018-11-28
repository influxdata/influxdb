// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {IconFont} from 'src/clockface'
import {SideBarTabStatus as TabStatus} from 'src/onboarding/components/side_bar/SideBar'

interface Props {
  label: string
  id: string
  active: boolean
  status: TabStatus
  onClick: (tabID: string) => void
}

class SideBarTab extends Component<Props> {
  public render() {
    const {label} = this.props

    return (
      <div className={this.className} onClick={this.handleClick}>
        {this.icon}
        {label}
      </div>
    )
  }

  private handleClick = (): void => {
    const {id, onClick} = this.props

    onClick(id)
  }

  private get className(): string {
    const {status, active} = this.props

    return classnames('side-bar--tab', {
      active,
      'side-bar--tab__success': status === TabStatus.Success,
      'side-bar--tab__error': status === TabStatus.Error,
      'side-bar--tab__pending': status === TabStatus.Pending,
    })
  }

  private get icon(): JSX.Element {
    const {status} = this.props
    let icon

    switch (status) {
      case TabStatus.Pending:
      case TabStatus.Success:
        icon = `side-bar--icon icon ${IconFont.Checkmark}`
        break
      case TabStatus.Error:
        icon = `side-bar--icon icon ${IconFont.Remove}`
        break
      case TabStatus.Default:
        icon = `side-bar--icon icon ${IconFont.CircleThick}`
        break
      default:
        icon = `side-bar--icon`
    }

    return <span className={icon} />
  }
}

export default SideBarTab
