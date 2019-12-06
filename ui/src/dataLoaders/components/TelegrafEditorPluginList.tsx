import React, {PureComponent} from 'react'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
  TelegrafEditorActivePlugin,
  TelegrafEditorBasicPlugin,
  TelegrafEditorBundlePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

function groupPlugins(plugins, pluginFilter) {
  const map = plugins.reduce((prev, curr) => {
    if (curr.name === '__default__') {
      return prev
    }

    if (!prev.hasOwnProperty(curr.type)) {
      prev[curr.type] = []
    }

    prev[curr.type].push(curr)

    return prev
  }, {})

  return ['bundle', 'input', 'output', 'processor', 'aggregator']
    .map(k => {
      return {
        category: k,
        items: (map[k] || []).filter(a => a.name.indexOf(pluginFilter) > -1),
      }
    })
    .filter(k => k.items.length)
    .reduce((prev, curr) => {
      prev.push({
        type: 'display',
        name: '-- ' + curr.category + ' --',
      })

      const items = curr.items.slice(0).sort((a, b) => {
        return a.name.localeCompare(b.name)
      })

      prev.push(...items)

      return prev
    }, [])
}

type ListPlugin =
  | TelegrafEditorBasicPlugin
  | TelegrafEditorBundlePlugin
  | TelegrafEditorActivePlugin
interface PluginProps {
  plugins: TelegrafEditorPluginState | TelegrafEditorActivePluginState
  filter: string
  onClick: (which: ListPlugin) => void
}

class PluginList extends PureComponent<PluginProps> {
  render() {
    const {plugins, filter, onClick} = this.props
    const list = groupPlugins(plugins, filter).map(k => {
      if (k.type === 'display') {
        return (
          <div className={k.type} key={`_plugin_${k.type}.${k.name}`}>
            {k.name}
          </div>
        )
      }

      return (
        <div
          className={k.type}
          key={`_plugin_${k.type}.${k.name}`}
          onClick={() => onClick(k)}
        >
          {k.name}
          <label>{k.description}</label>
        </div>
      )
    })

    return (
      <FancyScrollbar autoHide={false} className="telegraf-editor--plugins">
        {list}
      </FancyScrollbar>
    )
  }
}

export default PluginList
