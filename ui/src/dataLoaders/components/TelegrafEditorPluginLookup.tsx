// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import classnames from 'classnames'

// Components
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {AppState} from 'src/types'
import {SquareButton, IconFont, ComponentSize} from '@influxdata/clockface'
import {setLookup} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {TelegrafEditorActivePlugin} from 'src/dataLoaders/reducers/telegrafEditor'

interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const TelegrafEditorSideBar: FC<Props> = ({
  plugins,
  onJump,
  show,
  onChangeLookup,
}) => {
  const columnClassName = classnames('telegraf-editor--right-column', {
    'telegraf-editor--column__collapsed': !show,
  })
  const icon = show ? IconFont.EyeOpen : IconFont.EyeClosed
  const header = 'Plugins'

  return (
    <div className={columnClassName}>
      <div className="telegraf-editor--column-heading">
        <span className="telegraf-editor--title">{header}</span>
        <SquareButton
          icon={icon}
          size={ComponentSize.ExtraSmall}
          onClick={() => onChangeLookup(!show)}
        />
      </div>
      {show && <PluginList plugins={plugins} filter="" onClick={onJump} />}
      {!show && (
        <span className="telegraf-editor--title__collapsed">{header} </span>
      )}
    </div>
  )
}

const mstp = (state: AppState) => {
  const plugins = state.telegrafEditorActivePlugins || []
  const show = state.telegrafEditor.showLookup

  return {plugins, show}
}

const mdtp = {
  onChangeLookup: setLookup,
}

const connector = connect(mstp, mdtp)

export default connector(TelegrafEditorSideBar)
