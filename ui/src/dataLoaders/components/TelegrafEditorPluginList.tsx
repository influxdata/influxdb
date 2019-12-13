// Libraries
import React, {PureComponent} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
  TelegrafEditorActivePlugin,
  TelegrafEditorPlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

type ListPlugin = TelegrafEditorPlugin | TelegrafEditorActivePlugin

interface InterimListFormat {
  category: string
  items: Array<ListPlugin>
}

function groupPlugins(plugins: Array<ListPlugin>, pluginFilter: string) {
  const map = plugins.reduce(
    (prev: {[k: string]: Array<ListPlugin>}, curr: ListPlugin) => {
      if (curr.name === '__default__') {
        return prev
      }

      if (!prev.hasOwnProperty(curr.type)) {
        prev[curr.type] = []
      }

      prev[curr.type].push(curr)

      return prev
    },
    {}
  )

  return ['bundle', 'input', 'output', 'processor', 'aggregator']
    .map(
      (k: string): InterimListFormat => {
        return {
          category: k,
          items: (map[k] || []).filter((a: ListPlugin) =>
            (a.name || '').includes(pluginFilter)
          ),
        }
      }
    )
    .filter((k: InterimListFormat) => k.items.length)
    .reduce((prev, curr) => {
      prev.push({
        type: 'display',
        name: curr.category,
      })

      const items = curr.items.slice(0).sort((a: ListPlugin, b: ListPlugin) => {
        return (a.name || '').localeCompare(b.name || '')
      })

      prev.push(...items)

      return prev
    }, [])
}

interface PluginProps {
  plugins: TelegrafEditorPluginState | TelegrafEditorActivePluginState
  filter: string
  onClick: (which: ListPlugin) => void
}

class PluginList extends PureComponent<PluginProps> {
  render() {
    const {plugins, filter, onClick} = this.props
    const list = groupPlugins(plugins, filter).map((k: ListPlugin) => {
      if (k.type === 'display') {
        return (
          <div
            className="telegraf-plugins--item telegraf-plugins--divider"
            key={`_plugin_${k.type}.${k.name}`}
          >
            {k.name}s
          </div>
        )
      }

      let description

      // NOTE: written this way to bypass typescript: alex
      if (k['description']) {
        description = (
          <dd className="telegraf-plugins--item-description">
            {k['description']}
          </dd>
        )
      }

      return (
        <div
          className="telegraf-plugins--item telegraf-plugins--plugin"
          key={`_plugin_${k.type}.${k.name}`}
          onClick={() => onClick(k)}
        >
          <dt className="telegraf-plugins--item-name">{k.name}</dt>
          {description}
        </div>
      )
    })

    return (
      <DapperScrollbars autoHide={false} className="telegraf-plugins">
        <dl className="telegraf-plugins--list">{list}</dl>
      </DapperScrollbars>
    )
  }
}

export default PluginList
