// Libraries
import React, {PureComponent} from 'react'
import {get} from 'lodash'

// Components
import {OverlayContainer, OverlayBody, OverlayHeading} from 'src/clockface'
import PermissionsWidget, {
  PermissionsWidgetMode,
  PermissionsWidgetSelection,
} from 'src/shared/components/permissionsWidget/PermissionsWidget'
import CodeSnippet from 'src/shared/components/CodeSnippet'

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
    const {description} = this.props.auth
    const {onNotify} = this.props

    const permissions = this.permissions

    return (
      <OverlayContainer>
        <OverlayHeading title={description} onDismiss={this.handleDismiss} />
        <OverlayBody>
          <CodeSnippet copyText={this.props.auth.token} notify={onNotify} />
          <PermissionsWidget
            mode={PermissionsWidgetMode.Read}
            heightPixels={500}
          >
            {Object.keys(permissions).map(type => {
              return (
                <PermissionsWidget.Section
                  key={type}
                  id={type}
                  title={type}
                  mode={PermissionsWidgetMode.Read}
                >
                  {permissions[type].map((action, i) => (
                    <PermissionsWidget.Item
                      key={i}
                      label={action}
                      id={this.itemID(type, action)}
                      selected={PermissionsWidgetSelection.Selected}
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

  private get permissions(): {[x: string]: Permission.ActionEnum[]} {
    const p = this.props.auth.permissions.reduce((acc, {action, resource}) => {
      const {type} = resource
      const name = get(resource, 'name', '')

      let key = `${type}-${name}`
      let actions = get(resource, key, [])

      if (name) {
        return {...acc, [key]: [...actions, action]}
      }

      actions = get(resource, type, [])

      return {...acc, [type]: [...actions, action]}
    }, {})

    return p
  }

  private itemID = (
    permission: string,
    action: Permission.ActionEnum
  ): string => {
    return `${permission}-${action}-${permission || '*'}-${permission || '*'}`
  }

  private handleDismiss = () => {
    this.props.onDismissOverlay()
  }
}
