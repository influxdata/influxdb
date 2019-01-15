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
import {Authorization, Permission} from 'src/api'

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

    return (
      <OverlayContainer>
        <OverlayHeading title={description} onDismiss={this.handleDismiss} />
        <OverlayBody>
          <CopyText copyText={this.props.auth.token} notify={onNotify} />
          <PermissionsWidget
            mode={PermissionsWidgetMode.Read}
            heightPixels={500}
          >
            {permissions.map((p, i) => {
              return (
                <PermissionsWidget.Section
                  key={i}
                  id={this.id(p)}
                  title={this.title(p)}
                  mode={PermissionsWidgetMode.Read}
                >
                  {actions.map((a, i) => (
                    <PermissionsWidget.Item
                      key={i}
                      id={this.itemID(p, a)}
                      label={a}
                      selected={this.selected(p, a)}
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
    permission: Permission,
    action: Permission.ActionEnum
  ): PermissionsWidgetSelection => {
    if (permission.action === action) {
      return PermissionsWidgetSelection.Selected
    }

    return PermissionsWidgetSelection.Unselected
  }

  private itemID = (
    permission: Permission,
    action: Permission.ActionEnum
  ): string => {
    return `${permission.id || permission.resource}-${action}`
  }

  private id = (permission: Permission): string => {
    return permission.id || permission.resource
  }

  private title = (permission): string => {
    if (permission.name) {
      return `${permission.resource}:${permission.name}`
    }

    return `${permission.resource}:*`
  }

  private handleDismiss = () => {
    this.props.onDismissOverlay()
  }
}
