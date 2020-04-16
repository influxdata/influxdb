// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Icon, IconFont} from '@influxdata/clockface'
import {SideBarTabStatus as TabStatus} from 'src/dataLoaders/components/side_bar/SideBar'

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
        <pre>
          {this.icon} {label}
        </pre>
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

    switch (status) {
      case TabStatus.Pending:
      case TabStatus.Success:
        return <Icon glyph={IconFont.Checkmark} />
      case TabStatus.Error:
        return <Icon glyph={IconFont.Remove} />
      default:
        return <Icon glyph={IconFont.CircleThick} />
    }
  }
}

export default SideBarTab
