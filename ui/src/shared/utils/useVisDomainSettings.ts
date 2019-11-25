// Libraries
import {useMemo} from 'react'
import {NumericColumnData} from '@influxdata/giraffe'

// Utils
import {useOneWayState} from 'src/shared/utils/useOneWayState'
import {extent} from 'src/shared/utils/vis'

/*
  This hook helps map the domain setting stored for line graph to the
  appropriate settings on a @influxdata/giraffe `Config` object. 

  If the domain setting is present, it should be used. If the domain setting is
  not present, then the min/max values shown should be derived from the data
  passed to the plot.
*/
export const useVisDomainSettings = (
  storedDomain: number[],
  data: NumericColumnData,
  startTime: number = Infinity,
  endTime: number = -Infinity
) => {
  const initialDomain = useMemo(() => {
    if (storedDomain) {
      return storedDomain
    }

    const range = extent((data as number[]) || [])
    const start = Math.min(startTime, range[0])
    const end = Math.max(endTime, range[1])

    return [start, end]
  }, [storedDomain, data])

  const [domain, setDomain] = useOneWayState(initialDomain)
  const resetDomain = () => setDomain(initialDomain)

  return [domain, setDomain, resetDomain]
}
