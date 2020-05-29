// Libraries
import React, {Component, CSSProperties} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import PermissionsWidgetSection from 'src/shared/components/permissionsWidget/PermissionsWidgetSection'
import PermissionsWidgetItem from 'src/shared/components/permissionsWidget/PermissionsWidgetItem'

export enum PermissionsWidgetMode {
  Read = 'read',
  Write = 'write',
}

export enum PermissionsWidgetSelection {
  Selected = 'selected',
  Unselected = 'unselected',
}

interface Props {
  children: JSX.Element[] | JSX.Element
  mode: PermissionsWidgetMode
  heightPixels: number
  className?: string
}

class PermissionsWidget extends Component<Props> {
  public static defaultProps = {
    heightPixels: 500,
  }

  public static Section = PermissionsWidgetSection
  public static Item = PermissionsWidgetItem

  public render() {
    return (
      <div className={this.className} style={this.style}>
        <div className="permissions-widget--header">{this.headerText}</div>
        <div className="permissions-widget--body">
          <DapperScrollbars autoHide={false}>{this.sections}</DapperScrollbars>
        </div>
      </div>
    )
  }

  private get className(): string {
    const {className} = this.props

    if (className) {
      return `permissions-widget ${className}`
    }

    return 'permissions-widget'
  }

  private get sections() {
    const {children, mode} = this.props

    return React.Children.map(children, (child: JSX.Element) => {
      if (child.type !== PermissionsWidgetSection) {
        return null
      }

      return <PermissionsWidgetSection {...child.props} mode={mode} />
    })
  }

  private get style(): CSSProperties {
    const {heightPixels} = this.props

    return {height: `${heightPixels}px`}
  }

  private get headerText(): string {
    const {mode} = this.props

    if (mode === PermissionsWidgetMode.Read) {
      return 'Summary of access permissions'
    } else if (mode === PermissionsWidgetMode.Write) {
      return 'Summary of access permissions, each item can be toggled ON or OFF'
    }
  }
}

export default PermissionsWidget
