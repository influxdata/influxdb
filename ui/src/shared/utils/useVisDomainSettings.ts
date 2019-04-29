// Libraries
import {useMemo} from 'react'

// Utils
import {useOneWayState} from 'src/shared/utils/useOneWayState'
import {extent} from 'src/shared/utils/vis'

/*
  This hook helps map the domain setting stored for line graph to the
  appropriate settings on a @influxdata/vis `Config` object. 

  If the domain setting is present, it should be used. If the domain setting is
  not present, then the min/max values shown should be derived from the data
  passed to the plot.
*/
export const useVisDomainSettings = (
  storedDomain: [number, number],
  data: number[]
) => {
  const initialDomain = useMemo(
    () => (storedDomain ? storedDomain : extent(data)),
    [storedDomain, data]
  )

  const [domain, setDomain] = useOneWayState(initialDomain)
  const resetDomain = () => setDomain(initialDomain)

  return [domain, setDomain, resetDomain]
}
