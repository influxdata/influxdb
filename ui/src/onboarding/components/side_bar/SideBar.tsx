// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import SideBarTab from 'src/onboarding/components/side_bar/SideBarTab'
import SideBarButton from 'src/onboarding/components/side_bar/SideBarButton'
import {ComponentSpacer, Direction, Alignment} from 'src/clockface'

// Styles
import './SideBar.scss'

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
          <div className="side-bar--contents">
            <div className="side-bar--tabs">{this.childTabs}</div>
            <div className="side-bar--buttons">
              <ComponentSpacer
                direction={Direction.Vertical}
                align={Alignment.Left}
              >
                {this.childButtons}
              </ComponentSpacer>
            </div>
          </div>
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

  private get childButtons(): JSX.Element[] {
    const {children} = this.props
    return React.Children.map(children, (child: JSX.Element) => {
      if (child.type === SideBarButton) {
        return child
      }
    })
  }
}

export default SideBar
