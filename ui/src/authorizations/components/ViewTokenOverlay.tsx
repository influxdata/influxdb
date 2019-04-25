// Libraries
import React, {PureComponent} from 'react'
import {get} from 'lodash'

// Components
import {Overlay} from 'src/clockface'
import PermissionsWidget, {
  PermissionsWidgetMode,
  PermissionsWidgetSelection,
} from 'src/shared/components/permissionsWidget/PermissionsWidget'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Types
import {Authorization, Permission} from '@influxdata/influx'

interface Props {
  auth: Authorization
  onDismissOverlay: () => void
}

export default class ViewTokenOverlay extends PureComponent<Props> {
  public render() {
    const {description} = this.props.auth

    const permissions = this.permissions

    return (
      <Overlay.Container>
        <Overlay.Heading title={description} onDismiss={this.handleDismiss} />
        <Overlay.Body>
          <CodeSnippet copyText={this.props.auth.token} />
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
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private get permissions(): {[x: string]: Permission.ActionEnum[]} {
    const p = this.props.auth.permissions.reduce((acc, {action, resource}) => {
      const {type} = resource
      const name = get(resource, 'name', '')
      let key = `${type}`
      if (name) {
        key = `${type}-${name}`
      }

      let actions = get(acc, key, [])

      if (name && actions) {
        return {...acc, [key]: [...actions, action]}
      }

      actions = get(acc, key || resource.type, [])
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
