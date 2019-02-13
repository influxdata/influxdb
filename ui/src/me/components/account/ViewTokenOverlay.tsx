// Libraries
import React, {PureComponent} from 'react'

// Components
import {OverlayContainer, OverlayBody, OverlayHeading} from 'src/clockface'
import PermissionsWidget, {
  PermissionsWidgetMode,
  PermissionsWidgetSelection,
} from 'src/shared/components/permissionsWidget/PermissionsWidget'
import CopyText from 'src/shared/components/CopyText'

// Types
import {Authorization, Permission} from '@influxdata/influx'

// Actions
import {NotificationAction} from 'src/types'

const {Write, Read} = Permission.ActionEnum

interface Props {
  onNotify: NotificationAction
  auth: Authorization
  onDismissOverlay: () => void
}

const actions = [Read, Write]

export default class ViewTokenOverlay extends PureComponent<Props> {
  public render() {
    const {description, permissions} = this.props.auth
    const {onNotify} = this.props

    const permissionsByType = {}
    for (const key of permissions) {
      if (permissionsByType[key.resource.type]) {
        permissionsByType[key.resource.type].push(key.action)
      } else {
        permissionsByType[key.resource.type] = [key.action]
      }
    }

    return (
      <OverlayContainer>
        <OverlayHeading title={description} onDismiss={this.handleDismiss} />
        <OverlayBody>
          <CopyText copyText={this.props.auth.token} notify={onNotify} />
          <PermissionsWidget
            mode={PermissionsWidgetMode.Read}
            heightPixels={500}
          >
            {Object.keys(permissionsByType).map((type, permission) => {
              return (
                <PermissionsWidget.Section
                  key={permission}
                  id={type}
                  title={this.title(type)}
                  mode={PermissionsWidgetMode.Read}
                >
                  {actions.map((a, i) => (
                    <PermissionsWidget.Item
                      key={i}
                      id={this.itemID(type, a)}
                      label={a}
                      selected={this.selected(permissionsByType[type][i], a)}
                    />
                  ))}
                </PermissionsWidget.Section>
              )
            })}
          </PermissionsWidget>
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private selected = (
    permission: string,
    action: Permission.ActionEnum
  ): PermissionsWidgetSelection => {
    if (permission === action) {
      return PermissionsWidgetSelection.Selected
    }

    return PermissionsWidgetSelection.Unselected
  }

  private itemID = (
    permission: string,
    action: Permission.ActionEnum
  ): string => {
    return `${permission}-${action}-${permission || '*'}-${permission || '*'}`
  }

  private title = (permission: string): string => {
    return `${permission}:*`
  }

  private handleDismiss = () => {
    this.props.onDismissOverlay()
  }
}
