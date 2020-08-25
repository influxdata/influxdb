// Libraries
import React, {FC, useContext, useEffect} from 'react'
import {useParams} from 'react-router-dom'

// Components
import {Page} from '@influxdata/clockface'
import NotebookHeader from 'src/notebooks/components/header'
import PipeList from 'src/notebooks/components/PipeList'
import MiniMap from 'src/notebooks/components/minimap/MiniMap'

// Contexts
import {ResultsProvider} from 'src/notebooks/context/results'
import {RefProvider} from 'src/notebooks/context/refs'
import CurrentNotebookProvider, {
  NotebookContext,
} from 'src/notebooks/context/notebook.current'
import {ScrollProvider} from 'src/notebooks/context/scroll'

const NotebookFromRoute = () => {
  const {id} = useParams()
  const {change} = useContext(NotebookContext)

  useEffect(() => {
    change(id)
  }, [id, change])

  return null
}
// NOTE: uncommon, but using this to scope the project
// within the page and not bleed it's dependancies outside
// of the feature flag
import 'src/notebooks/style.scss'

const NotebookPage: FC = () => {
  return (
    <CurrentNotebookProvider>
      <NotebookFromRoute />
      <ResultsProvider>
        <RefProvider>
          <ScrollProvider>
            <Page titleTag="Flows">
              <NotebookHeader />
              <Page.Contents
                fullWidth={true}
                scrollable={false}
                className="notebook-page"
              >
                <div className="notebook">
                  <MiniMap />
                  <PipeList />
                </div>
              </Page.Contents>
            </Page>
          </ScrollProvider>
        </RefProvider>
      </ResultsProvider>
    </CurrentNotebookProvider>
  )
}

export default NotebookPage
