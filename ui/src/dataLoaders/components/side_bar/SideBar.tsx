// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import SideBarTab from 'src/dataLoaders/components/side_bar/SideBarTab'
import SideBarButton from 'src/dataLoaders/components/side_bar/SideBarButton'

export enum SideBarTabStatus {
  Default = 'default',
  Error = 'error',
  Success = 'success',
  Pending = 'pending',
  Blank = 'blank',
}

interface Props {
  title: string
  children: JSX.Element[]
  visible: boolean
}

class SideBar extends Component<Props> {
  public static Tab = SideBarTab
  public static Button = SideBarButton

  public render() {
    const {title} = this.props

    return (
      <div className={this.containerClassName}>
        <div className="side-bar--container">
          <h3 className="side-bar--title">{title}</h3>
          <DapperScrollbars autoHide={false}>
            <div className="side-bar--tabs">{this.childTabs}</div>
          </DapperScrollbars>
        </div>
      </div>
    )
  }

  private get containerClassName(): string {
    const {visible} = this.props

    return classnames('side-bar', {show: visible})
  }

  private get childTabs(): JSX.Element[] {
    const {children} = this.props
    return React.Children.map(children, (child: JSX.Element) => {
      if (child.type === SideBarTab) {
        return child
      }
    })
  }
}

export default SideBar
